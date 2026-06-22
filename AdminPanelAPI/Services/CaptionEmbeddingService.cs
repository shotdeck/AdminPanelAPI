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

                    var result = await ProcessImageAsync(httpClient, imageBytes, rec.Filename, metadata, false, cancellationToken);

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

        public async Task ProcessAllAsync(
            long jobId,
            int concurrency,
            CancellationToken cancellationToken,
            bool skipCaption = false)
        {
            const int chunkSize = 2000;

            _logger.LogInformation(
                "Starting process-all. JobId={JobId}, Concurrency={Concurrency}",
                jobId, concurrency);

            await _repo.UpdateProgressAsync(jobId, "Counting unprocessed images", null, null, cancellationToken);

            var totalRemaining = await _repo.GetUnprocessedCountAsync(cancellationToken);

            if (totalRemaining == 0)
            {
                await _repo.UpdateProgressAsync(jobId, "No unprocessed images found", 0, 0, cancellationToken);
                _logger.LogInformation("JobId={JobId}: No unprocessed images to process.", jobId);
                return;
            }

            _logger.LogInformation("JobId={JobId}: {Total} unprocessed images found.", jobId, totalRemaining);

            var totalProcessed = 0;
            var totalFailed = 0;
            var grandTotal = totalRemaining;

            await _repo.UpdateProgressAsync(jobId, "Processing images", 0, grandTotal, cancellationToken);

            // Pipeline: fetch the first chunk's data, then overlap fetching with processing
            var currentChunk = await FetchChunkWithMetadataAsync(chunkSize, cancellationToken);

            using var semaphore = new SemaphoreSlim(concurrency);
            var activeTasks = new List<Task>();

            while (currentChunk != null && !cancellationToken.IsCancellationRequested)
            {
                // Start fetching next chunk in background while we process current chunk
                var nextChunkTask = Task.Run(
                    () => FetchChunkWithMetadataAsync(chunkSize, cancellationToken),
                    cancellationToken);

                // Process current chunk
                foreach (var rec in currentChunk.Images)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (rec.Filename == null)
                    {
                        Interlocked.Increment(ref totalFailed);
                        continue;
                    }

                    await semaphore.WaitAsync(cancellationToken);

                    var chunk = currentChunk; // capture for closure
                    activeTasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            var httpClient = _httpClientFactory.CreateClient("HighConcurrency");
                            httpClient.Timeout = TimeSpan.FromMinutes(5);

                            var imageBytes = await DownloadImageAsync(httpClient, rec.Filename, cancellationToken);

                            var metadata = KeywordMetadataBuilder.BuildMetadata(
                                rec, chunk.Gender, chunk.SubjectAge, chunk.SubjectEth,
                                chunk.FrameSize, chunk.Tags, chunk.TimeOfDay, chunk.MovieFields);

                            var result = await ProcessImageAsync(httpClient, imageBytes, rec.Filename, metadata, skipCaption, cancellationToken);

                            await InsertCaptionEmbeddingAsync(
                                rec.Id, rec.Randid, result.Caption, result.Embeddings, result.FusedEmbeddings, metadata, cancellationToken);

                            var current = Interlocked.Increment(ref totalProcessed);

                            if (current % 100 == 0)
                            {
                                _logger.LogInformation(
                                    "JobId={JobId}: Progress {Processed}/{Total} ({Failed} failed)",
                                    jobId, current, grandTotal, totalFailed);

                                await _repo.UpdateProgressAsync(
                                    jobId,
                                    $"Processing: {current}/{grandTotal} ({totalFailed} failed)",
                                    current, grandTotal, cancellationToken);
                            }
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref totalFailed);
                            _logger.LogError(ex, "JobId={JobId}: Failed to process image {Idnum}", jobId, rec.Id);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }, cancellationToken));
                }

                // Wait for next chunk to be ready (should already be done by now)
                currentChunk = await nextChunkTask;

                // Clean up completed tasks periodically to avoid memory buildup
                if (activeTasks.Count > concurrency * 4)
                {
                    activeTasks.RemoveAll(t => t.IsCompleted);
                }
            }

            // Wait for all remaining tasks to complete
            await Task.WhenAll(activeTasks);

            await _repo.UpdateProgressAsync(
                jobId,
                $"Completed: {totalProcessed} processed, {totalFailed} failed out of {grandTotal}",
                totalProcessed, grandTotal, cancellationToken);

            _logger.LogInformation(
                "JobId={JobId}: Process-all complete. Processed={Processed}, Failed={Failed}, Total={Total}",
                jobId, totalProcessed, totalFailed, grandTotal);
        }

        private async Task<ChunkData?> FetchChunkWithMetadataAsync(int chunkSize, CancellationToken cancellationToken)
        {
            var images = await _repo.GetUnprocessedImagesAsync(chunkSize, cancellationToken);

            if (images.Count == 0)
                return null;

            var imageIds = images.Select(r => r.Id).ToList();
            var movieIds = images.Where(r => r.MovieId.HasValue).Select(r => r.MovieId!.Value).Distinct().ToList();

            // Fetch all metadata in parallel
            var genderTask = _repo.FetchTagsAsync("frl_join_images_gender", "gender", imageIds, cancellationToken);
            var subjectAgeTask = _repo.FetchTagsAsync("frl_join_images_subject_age", "subject_age", imageIds, cancellationToken);
            var subjectEthTask = _repo.FetchTagsAsync("frl_join_images_subject_ethnicity", "subject_ethnicity", imageIds, cancellationToken);
            var frameSizeTask = _repo.FetchTagsAsync("frl_join_images_frame_size", "frame_size", imageIds, cancellationToken);
            var tagsTask = _repo.FetchTagsAsync("frl_join_images_tags", "tag", imageIds, cancellationToken);
            var timeOfDayTask = _repo.FetchTagsAsync("frl_join_images_time_of_day", "time_of_day", imageIds, cancellationToken);
            var movieFieldsTask = _repo.FetchMovieFieldsAsync(movieIds, cancellationToken);

            await Task.WhenAll(genderTask, subjectAgeTask, subjectEthTask, frameSizeTask, tagsTask, timeOfDayTask, movieFieldsTask);

            return new ChunkData
            {
                Images = images,
                Gender = await genderTask,
                SubjectAge = await subjectAgeTask,
                SubjectEth = await subjectEthTask,
                FrameSize = await frameSizeTask,
                Tags = await tagsTask,
                TimeOfDay = await timeOfDayTask,
                MovieFields = await movieFieldsTask,
            };
        }

        private sealed class ChunkData
        {
            public required List<ImageRecord> Images { get; init; }
            public required Dictionary<int, List<string>> Gender { get; init; }
            public required Dictionary<int, List<string>> SubjectAge { get; init; }
            public required Dictionary<int, List<string>> SubjectEth { get; init; }
            public required Dictionary<int, List<string>> FrameSize { get; init; }
            public required Dictionary<int, List<string>> Tags { get; init; }
            public required Dictionary<int, List<string>> TimeOfDay { get; init; }
            public required Dictionary<int, (string? Title, string? Year, string? Genre, string? Product, string? Brand, string? MediaType, string? Director, string? Cinematographer)> MovieFields { get; init; }
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

                var apiResult = await ProcessImageAsync(httpClient, imageBytes, rec.Filename, metadata, false, cancellationToken);

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
            bool skipCaption,
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

            if (skipCaption)
            {
                content.Add(new StringContent("true"), "skip_caption");
            }

            var response = await httpClient.PostAsync(url, content, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(responseBody);

            string? caption = null;
            if (doc.RootElement.TryGetProperty("caption", out var captionProp)
                && captionProp.ValueKind == JsonValueKind.String)
            {
                caption = captionProp.GetString();
            }

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
            public string? Caption { get; set; }
            public float[] Embeddings { get; set; } = Array.Empty<float>();
            public float[]? FusedEmbeddings { get; set; }
        }

        private async Task InsertCaptionEmbeddingAsync(
            int idnum,
            string? randid,
            string? caption,
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
            cmd.Parameters.AddWithValue("captions", (object?)caption ?? DBNull.Value);

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
