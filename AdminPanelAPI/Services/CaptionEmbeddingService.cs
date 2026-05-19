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
                                 ?? "https://semanticsearch--ops-mm-embedding-embeddingservice-create-c9f1b2.modal.run";
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

            var images = await _repo.GetUnprocessedImageIdsAsync(batchSize, cancellationToken);

            if (images.Count == 0)
            {
                await _repo.UpdateProgressAsync(jobId, "No unprocessed images found", 0, 0, cancellationToken);
                _logger.LogInformation("JobId={JobId}: No unprocessed images found.", jobId);
                return;
            }

            var total = images.Count;
            var processed = 0;
            var failed = 0;

            await _repo.UpdateProgressAsync(jobId, "Processing images", 0, total, cancellationToken);

            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromMinutes(5);

            foreach (var (idnum, filename, randid) in images)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    await _repo.UpdateProgressAsync(
                        jobId,
                        $"Processing image {processed + 1}/{total} (idnum={idnum})",
                        processed,
                        total,
                        cancellationToken);

                    var imageBytes = await DownloadImageAsync(httpClient, filename, cancellationToken);

                    var caption = await GetImageCaptionAsync(httpClient, imageBytes, filename, cancellationToken);

                    var embeddings = await GetImageEmbeddingAsync(httpClient, imageBytes, filename, cancellationToken);

                    await InsertCaptionEmbeddingAsync(idnum, randid, caption, embeddings, cancellationToken);

                    processed++;

                    _logger.LogInformation(
                        "JobId={JobId}: Processed image {Idnum} ({Processed}/{Total})",
                        jobId, idnum, processed, total);
                }
                catch (Exception ex)
                {
                    failed++;
                    _logger.LogError(
                        ex,
                        "JobId={JobId}: Failed to process image {Idnum}",
                        jobId, idnum);
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

        private async Task<string> GetImageCaptionAsync(
            HttpClient httpClient,
            byte[] imageBytes,
            string filename,
            CancellationToken cancellationToken)
        {
            var url = $"{_captionApiBaseUrl.TrimEnd('/')}/get_image_caption";

            using var content = new MultipartFormDataContent();
            var imageContent = new ByteArrayContent(imageBytes);
            imageContent.Headers.ContentType = new MediaTypeHeaderValue("image/jpeg");
            content.Add(imageContent, "image", $"small_{filename}");

            var response = await httpClient.PostAsync(url, content, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(responseBody);

            return doc.RootElement.GetProperty("caption").GetString()
                   ?? throw new Exception("Caption response missing 'caption' field.");
        }

        private async Task<float[]> GetImageEmbeddingAsync(
            HttpClient httpClient,
            byte[] imageBytes,
            string filename,
            CancellationToken cancellationToken)
        {
            var url = $"{_captionApiBaseUrl.TrimEnd('/')}/get_image_embedding";

            using var content = new MultipartFormDataContent();
            var imageContent = new ByteArrayContent(imageBytes);
            imageContent.Headers.ContentType = new MediaTypeHeaderValue("image/jpeg");
            content.Add(imageContent, "image", $"small_{filename}");

            var response = await httpClient.PostAsync(url, content, cancellationToken);
            response.EnsureSuccessStatusCode();

            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            using var doc = JsonDocument.Parse(responseBody);

            var embeddingsArray = doc.RootElement.GetProperty("embeddings");
            var embeddings = new float[embeddingsArray.GetArrayLength()];

            int i = 0;
            foreach (var element in embeddingsArray.EnumerateArray())
            {
                embeddings[i++] = element.GetSingle();
            }

            return embeddings;
        }

        private async Task InsertCaptionEmbeddingAsync(
            int idnum,
            string? randid,
            string caption,
            float[] embeddings,
            CancellationToken cancellationToken)
        {
            const string sql = @"
INSERT INTO frl.frl_caption_embeddings (idnum, randid, captions, vectorized_embeddings, status)
VALUES (@idnum, @randid, @captions, @embeddings::halfvec, 'completed')
ON CONFLICT (idnum) DO NOTHING;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("idnum", idnum);
            cmd.Parameters.AddWithValue("randid", (object?)randid ?? DBNull.Value);
            cmd.Parameters.AddWithValue("captions", caption);

            var embeddingString = "[" + string.Join(",", embeddings) + "]";
            cmd.Parameters.AddWithValue("embeddings", embeddingString);

            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
