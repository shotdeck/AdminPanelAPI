using AdminPanelAPI.Helpers;
using AdminPanelAPI.Models;
using Npgsql;
using NpgsqlTypes;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    public class CaptionEmbeddingService : ICaptionEmbeddingService
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<CaptionEmbeddingService> _logger;
        private readonly ICaptionEmbeddingJobRepository _repo;

        private readonly string _connectionString;
        private readonly string _imageServerBaseUrl;
        private readonly string _imageServerLogin;
        private readonly string _imageServerPassword;
        private readonly string _captionApiBaseUrl;

        public CaptionEmbeddingService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<CaptionEmbeddingService> logger,
            ICaptionEmbeddingJobRepository repo)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _repo = repo;

            _connectionString = _configuration.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing connection string: Default");

            _imageServerBaseUrl = _configuration["CaptionEmbedding:ImageServerBaseUrl"]
                                  ?? "http://35.89.51.60:8889";

            _imageServerLogin = _configuration["CaptionEmbedding:ImageServerLogin"] ?? "";
            _imageServerPassword = _configuration["CaptionEmbedding:ImageServerPassword"] ?? "";

            _captionApiBaseUrl = _configuration["CaptionEmbedding:CaptionApiBaseUrl"]
                                 ?? "https://semanticsearch--qwen3-vl-embedding-api-api.modal.run";
        }

        public async Task ProcessBatchAsync(
            long jobId,
            int batchSize,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Starting caption embedding batch. JobId={JobId}, BatchSize={BatchSize}",
                jobId, batchSize);

            await _repo.UpdateProgressAsync(jobId, "Fetching unprocessed images", null, null, cancellationToken);

            var images = await _repo.GetUnprocessedImagesAsync(batchSize, cancellationToken);

            if (images.Count == 0)
            {
                await _repo.UpdateProgressAsync(jobId, "No unprocessed images found", 0, 0, cancellationToken);
                _logger.LogInformation("JobId={JobId}: No unprocessed images found.", jobId);
                return;
            }

            var total = images.Count;
            var processed = 0;
            var failed = 0;

            // Pre-fetch metadata for the whole batch
            await _repo.UpdateProgressAsync(jobId, "Fetching tags and movie metadata", null, null, cancellationToken);

            var imageIds = images.Select(r => r.Id).ToList();
            var movieIds = images.Where(r => r.MovieId.HasValue).Select(r => r.MovieId!.Value).Distinct().ToList();

            var gender = await _repo.FetchTagsAsync("frl_join_images_gender", "gender", imageIds, cancellationToken);
            var subjectAge = await _repo.FetchTagsAsync("frl_join_images_subject_age", "subject_age", imageIds, cancellationToken);
            var subjectEth = await _repo.FetchTagsAsync("frl_join_images_subject_ethnicity", "subject_ethnicity", imageIds, cancellationToken);
            var frameSize = await _repo.FetchTagsAsync("frl_join_images_frame_size", "frame_size", imageIds, cancellationToken);
            var tags = await _repo.FetchTagsAsync("frl_join_images_tags", "tag", imageIds, cancellationToken);
            var timeOfDay = await _repo.FetchTagsAsync("frl_join_images_time_of_day", "time_of_day", imageIds, cancellationToken);
            var movieFields = await _repo.FetchMovieFieldsAsync(movieIds, cancellationToken);

            await _repo.UpdateProgressAsync(jobId, "Processing images", 0, total, cancellationToken);

            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromMinutes(5);

            foreach (var rec in images)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (rec.Filename == null)
                {
                    failed++;
                    _logger.LogWarning("JobId={JobId}: Skipping image {Idnum} — no filename.", jobId, rec.Id);
                    continue;
                }

                try
                {
                    await _repo.UpdateProgressAsync(
                        jobId,
                        $"Processing image {processed + 1}/{total} (idnum={rec.Id})",
                        processed,
                        total,
                        cancellationToken);

                    var imageBytes = await DownloadImageAsync(httpClient, rec.Filename, cancellationToken);

                    var metadata = KeywordMetadataBuilder.BuildMetadata(
                        rec, gender, subjectAge, subjectEth,
                        frameSize, tags, timeOfDay, movieFields);

                    var result = await ProcessImageAsync(httpClient, imageBytes, rec.Filename, metadata, cancellationToken);

                    await InsertCaptionEmbeddingAsync(
                        rec.Id, rec.Randid, result.Caption, result.Embeddings, result.FusedEmbeddings, metadata, cancellationToken);

                    processed++;

                    _logger.LogInformation(
                        "JobId={JobId}: Processed image {Idnum} ({Processed}/{Total})",
                        jobId, rec.Id, processed, total);
                }
                catch (Exception ex)
                {
                    failed++;
                    _logger.LogError(
                        ex,
                        "JobId={JobId}: Failed to process image {Idnum}",
                        jobId, rec.Id);
                }
            }

            await _repo.UpdateProgressAsync(
                jobId,
                $"Completed: {processed} processed, {failed} failed out of {total}",
                processed,
                total,
                cancellationToken);

            _logger.LogInformation(
                "JobId={JobId}: Batch complete. Processed={Processed}, Failed={Failed}, Total={Total}",
                jobId, processed, failed, total);
        }

        public async Task<CaptionEmbeddingResult> ProcessSingleImageAsync(
            int imageId,
            CancellationToken cancellationToken)
        {
            var result = new CaptionEmbeddingResult { ImageId = imageId };

            try
            {
                var rec = await _repo.GetImageByIdAsync(imageId, cancellationToken);
                if (rec == null)
                {
                    result.Status = "Failed";
                    result.Error = $"Image {imageId} not found in frl_images.";
                    return result;
                }

                if (rec.Filename == null)
                {
                    result.Status = "Failed";
                    result.Error = $"Image {imageId} has no filename.";
                    return result;
                }

                var imageIds = new List<int> { rec.Id };
                var movieIds = rec.MovieId.HasValue
                    ? new List<int> { rec.MovieId.Value }
                    : new List<int>();

                var gender = await _repo.FetchTagsAsync("frl_join_images_gender", "gender", imageIds, cancellationToken);
                var subjectAge = await _repo.FetchTagsAsync("frl_join_images_subject_age", "subject_age", imageIds, cancellationToken);
                var subjectEth = await _repo.FetchTagsAsync("frl_join_images_subject_ethnicity", "subject_ethnicity", imageIds, cancellationToken);
                var frameSize = await _repo.FetchTagsAsync("frl_join_images_frame_size", "frame_size", imageIds, cancellationToken);
                var tags = await _repo.FetchTagsAsync("frl_join_images_tags", "tag", imageIds, cancellationToken);
                var timeOfDay = await _repo.FetchTagsAsync("frl_join_images_time_of_day", "time_of_day", imageIds, cancellationToken);
                var movieFields = await _repo.FetchMovieFieldsAsync(movieIds, cancellationToken);

                var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(5);

                var imageBytes = await DownloadImageAsync(httpClient, rec.Filename, cancellationToken);

                var metadata = KeywordMetadataBuilder.BuildMetadata(
                    rec, gender, subjectAge, subjectEth,
                    frameSize, tags, timeOfDay, movieFields);

                var apiResult = await ProcessImageAsync(httpClient, imageBytes, rec.Filename, metadata, cancellationToken);

                await InsertCaptionEmbeddingAsync(
                    rec.Id, rec.Randid, apiResult.Caption, apiResult.Embeddings, apiResult.FusedEmbeddings, metadata, cancellationToken);

                result.Status = "Completed";
                result.Caption = apiResult.Caption;
                result.KeywordMetadata = metadata;
                result.EmbeddingLength = apiResult.Embeddings.Length;
                result.FusedEmbeddingLength = apiResult.FusedEmbeddings?.Length;

                _logger.LogInformation("Processed single image {ImageId} successfully.", imageId);
            }
            catch (Exception ex)
            {
                result.Status = "Failed";
                result.Error = ex.Message;
                _logger.LogError(ex, "Failed to process single image {ImageId}.", imageId);
            }

            return result;
        }

        private async Task<byte[]> DownloadImageAsync(
            HttpClient httpClient,
            string filename,
            CancellationToken cancellationToken)
        {
            var imageUrl = $"{_imageServerBaseUrl.TrimEnd('/')}/file/small_{filename}";

            var request = new HttpRequestMessage(HttpMethod.Get, imageUrl);

            if (!string.IsNullOrWhiteSpace(_imageServerLogin))
            {
                var credentials = Convert.ToBase64String(
                    System.Text.Encoding.ASCII.GetBytes($"{_imageServerLogin}:{_imageServerPassword}"));
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
            }

            var response = await httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            return await response.Content.ReadAsByteArrayAsync(cancellationToken);
        }

        private async Task<ProcessImageResult> ProcessImageAsync(
            HttpClient httpClient,
            byte[] imageBytes,
            string filename,
            string? metadata,
            CancellationToken cancellationToken)
        {
            var url = $"{_captionApiBaseUrl.TrimEnd('/')}/process_image";

            using var content = new MultipartFormDataContent();
            var imageContent = new ByteArrayContent(imageBytes);
            imageContent.Headers.ContentType = new MediaTypeHeaderValue("image/jpeg");
            content.Add(imageContent, "image", $"small_{filename}");

            if (!string.IsNullOrWhiteSpace(metadata))
            {
                content.Add(new StringContent(metadata), "metadata");
            }

            var response = await httpClient.PostAsync(url, content, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(responseBody);

            var caption = doc.RootElement.GetProperty("caption").GetString()
                          ?? throw new Exception("Response missing 'caption' field.");

            var embeddingsArray = doc.RootElement.GetProperty("embeddings");
            var embeddings = new float[embeddingsArray.GetArrayLength()];
            int i = 0;
            foreach (var element in embeddingsArray.EnumerateArray())
            {
                embeddings[i++] = element.GetSingle();
            }

            float[]? fusedEmbeddings = null;
            if (doc.RootElement.TryGetProperty("fused_embeddings", out var fusedProp)
                && fusedProp.ValueKind == JsonValueKind.Array)
            {
                fusedEmbeddings = new float[fusedProp.GetArrayLength()];
                int j = 0;
                foreach (var element in fusedProp.EnumerateArray())
                {
                    fusedEmbeddings[j++] = element.GetSingle();
                }
            }

            return new ProcessImageResult
            {
                Caption = caption,
                Embeddings = embeddings,
                FusedEmbeddings = fusedEmbeddings,
            };
        }

        private class ProcessImageResult
        {
            public string Caption { get; set; } = "";
            public float[] Embeddings { get; set; } = Array.Empty<float>();
            public float[]? FusedEmbeddings { get; set; }
        }

        private async Task InsertCaptionEmbeddingAsync(
            int idnum,
            string? randid,
            string caption,
            float[] embeddings,
            float[]? fusedEmbeddings,
            string? keywordMetadata,
            CancellationToken cancellationToken)
        {
            const string sql = @"
INSERT INTO frl.frl_caption_embeddings_qwen3 (idnum, randid, captions, vectorized_embeddings, fused_embeddings, keyword_vectorized_metadata, status)
VALUES (@idnum, @randid, @captions, @embeddings::halfvec, @fused::halfvec, @metadata, 'completed')
ON CONFLICT (idnum) DO UPDATE SET
    captions = EXCLUDED.captions,
    vectorized_embeddings = EXCLUDED.vectorized_embeddings,
    fused_embeddings = EXCLUDED.fused_embeddings,
    keyword_vectorized_metadata = EXCLUDED.keyword_vectorized_metadata,
    status = EXCLUDED.status;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("idnum", idnum);
            cmd.Parameters.AddWithValue("randid", (object?)randid ?? DBNull.Value);
            cmd.Parameters.AddWithValue("captions", caption);

            var embeddingString = "[" + string.Join(",", embeddings) + "]";
            cmd.Parameters.AddWithValue("embeddings", embeddingString);

            if (fusedEmbeddings != null)
            {
                var fusedString = "[" + string.Join(",", fusedEmbeddings) + "]";
                cmd.Parameters.AddWithValue("fused", fusedString);
            }
            else
            {
                cmd.Parameters.AddWithValue("fused", DBNull.Value);
            }

            cmd.Parameters.AddWithValue("metadata",
                (object?)keywordMetadata ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
