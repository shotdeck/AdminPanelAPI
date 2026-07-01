using AdminPanelAPI.Models;
using Npgsql;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace AdminPanelAPI.Services
{
    public class DialogueTranscriptionService : IDialogueTranscriptionService
    {
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<DialogueTranscriptionService> _logger;
        private readonly IDialogueTranscriptionJobRepository _repo;

        private readonly string _connectionString;
        private readonly string _transcriptionApiBaseUrl;

        public DialogueTranscriptionService(
            IConfiguration configuration,
            IHttpClientFactory httpClientFactory,
            ILogger<DialogueTranscriptionService> logger,
            IDialogueTranscriptionJobRepository repo)
        {
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _logger = logger;
            _repo = repo;

            _connectionString = _configuration.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing connection string: Default");

            _transcriptionApiBaseUrl = _configuration["DialogueSearch:TranscriptionApiBaseUrl"]
                ?? "http://localhost:8000";
        }

        public async Task TranscribeMovieAsync(
            long jobId,
            int movieId,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Starting dialogue transcription. JobId={JobId}, MovieId={MovieId}",
                jobId, movieId);

            await _repo.UpdateProgressAsync(jobId, "Sending to transcription API", 5, cancellationToken);

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(60);

            var url = $"{_transcriptionApiBaseUrl.TrimEnd('/')}/transcribe?movie_id={movieId}";

            using var response = await client.PostAsync(url, null, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception(
                    $"Transcription API failed. Status: {(int)response.StatusCode}, Body: {content}");
            }

            await _repo.UpdateProgressAsync(jobId, "Parsing transcription results", 70, cancellationToken);

            var result = JsonSerializer.Deserialize<TranscriptionApiResponse>(content);

            if (result == null || result.Error != null)
            {
                throw new Exception(
                    $"Transcription API returned error: {result?.Error ?? "empty response"}");
            }

            await _repo.UpdateProgressAsync(jobId, "Storing words in database", 80, cancellationToken);

            var wordCount = await InsertTranscriptWordsAsync(
                movieId, result.Words, cancellationToken);

            await _repo.UpdateProgressAsync(jobId, "Completed", 100, cancellationToken);

            _logger.LogInformation(
                "Dialogue transcription complete. JobId={JobId}, MovieId={MovieId}, Words={WordCount}",
                jobId, movieId, wordCount);
        }

        private async Task<int> InsertTranscriptWordsAsync(
            int movieId,
            List<TranscriptionWord> words,
            CancellationToken cancellationToken)
        {
            if (words.Count == 0) return 0;

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            // Delete existing words for this movie (idempotent re-transcription)
            await using (var deleteCmd = new NpgsqlCommand(
                "DELETE FROM frl.frl_transcript_words WHERE movieid = @movieid", conn))
            {
                deleteCmd.Parameters.AddWithValue("movieid", movieId);
                await deleteCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            var wordIndex = 0;
            var insertedCount = 0;

            // Batch insert in groups of 500
            const int batchSize = 500;

            for (var i = 0; i < words.Count; i += batchSize)
            {
                var batch = words.Skip(i).Take(batchSize).ToList();

                await using var tx = await conn.BeginTransactionAsync(cancellationToken);

                try
                {
                    foreach (var w in batch)
                    {
                        var cleaned = CleanWord(w.Word);
                        if (string.IsNullOrEmpty(cleaned))
                            continue;

                        const string sql = @"
INSERT INTO frl.frl_transcript_words (movieid, word_index, word, start_time, end_time, confidence)
VALUES (@movieid, @word_index, @word, @start_time, @end_time, @confidence);";

                        await using var cmd = new NpgsqlCommand(sql, conn, tx);
                        cmd.Parameters.AddWithValue("movieid", movieId);
                        cmd.Parameters.AddWithValue("word_index", wordIndex);
                        cmd.Parameters.AddWithValue("word", cleaned);
                        cmd.Parameters.AddWithValue("start_time", Math.Round(w.Start, 3));
                        cmd.Parameters.AddWithValue("end_time", Math.Round(w.End, 3));
                        cmd.Parameters.AddWithValue("confidence",
                            w.Probability.HasValue ? (object)Math.Round(w.Probability.Value, 4) : DBNull.Value);

                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                        wordIndex++;
                        insertedCount++;
                    }

                    await tx.CommitAsync(cancellationToken);
                }
                catch
                {
                    await tx.RollbackAsync(cancellationToken);
                    throw;
                }
            }

            return insertedCount;
        }

        private static string CleanWord(string word)
        {
            var cleaned = Regex.Replace(word.ToLowerInvariant(), @"[^\w']", "").Trim('\'');
            return cleaned;
        }
    }
}
