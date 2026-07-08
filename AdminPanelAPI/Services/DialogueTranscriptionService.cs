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
            string? r2Key,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation(
                "Starting dialogue transcription. JobId={JobId}, MovieId={MovieId}, R2Key={R2Key}",
                jobId, movieId, r2Key);

            // Skip re-transcription if this movie already has words (e.g. a
            // backfill job that only needs segmenting).
            var existingWordCount = await GetWordCountAsync(movieId, cancellationToken);

            int wordCount;
            if (existingWordCount > 0)
            {
                _logger.LogInformation(
                    "Movie {MovieId} already has {Count} words; skipping transcription.",
                    movieId, existingWordCount);
                wordCount = existingWordCount;
            }
            else
            {
                await _repo.UpdateProgressAsync(jobId, "Sending to transcription API", 5, cancellationToken);

                // The Modal /transcribe endpoint requires r2_key. A re-transcribe
                // (transcribe/{id}?force=true) has no r2Key on the job, so resolve
                // it from the movie's most recent job here.
                r2Key ??= await GetR2KeyForMovieAsync(movieId, cancellationToken);
                if (string.IsNullOrWhiteSpace(r2Key))
                    throw new Exception($"No r2_key found for movie {movieId}; cannot transcribe.");

                var client = _httpClientFactory.CreateClient();
                client.Timeout = TimeSpan.FromMinutes(60);

                var url = $"{_transcriptionApiBaseUrl.TrimEnd('/')}/transcribe?movie_id={movieId}"
                    + $"&r2_key={Uri.EscapeDataString(r2Key)}";

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

                wordCount = await InsertTranscriptWordsAsync(
                    movieId, result.Words, cancellationToken);
            }

            // Split the movie into ~10s segments and map each word to its segment
            // so playback loads tiny self-contained files instead of seeking a 2 GB file.
            await SegmentMovieAsync(jobId, movieId, r2Key, cancellationToken);

            await _repo.UpdateProgressAsync(jobId, "Completed", 100, cancellationToken);

            _logger.LogInformation(
                "Dialogue transcription complete. JobId={JobId}, MovieId={MovieId}, Words={WordCount}",
                jobId, movieId, wordCount);
        }

        /// <summary>
        /// Ask the Modal service to split the movie into segments (stored in R2),
        /// then map each transcript word to its segment (index + real start offset).
        /// Idempotent: skips if the movie is already segmented.
        /// </summary>
        private async Task SegmentMovieAsync(
            long jobId,
            int movieId,
            string? r2Key,
            CancellationToken cancellationToken)
        {
            if (await IsMovieSegmentedAsync(movieId, cancellationToken))
            {
                _logger.LogInformation("Movie {MovieId} already segmented; skipping.", movieId);
                return;
            }

            r2Key ??= await GetR2KeyForMovieAsync(movieId, cancellationToken);
            if (string.IsNullOrWhiteSpace(r2Key))
            {
                _logger.LogWarning(
                    "No r2_key for movie {MovieId}; cannot segment.", movieId);
                return;
            }

            await _repo.UpdateProgressAsync(jobId, "Splitting movie into segments", 90, cancellationToken);

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromMinutes(60);

            var url = $"{_transcriptionApiBaseUrl.TrimEnd('/')}/segment"
                + $"?movie_id={movieId}&r2_key={Uri.EscapeDataString(r2Key)}";

            using var response = await client.PostAsync(url, null, cancellationToken);
            var content = await response.Content.ReadAsStringAsync(cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                throw new Exception(
                    $"Segment API failed. Status: {(int)response.StatusCode}, Body: {content}");
            }

            var manifest = JsonSerializer.Deserialize<SegmentManifestResponse>(content);
            if (manifest == null || manifest.Segments.Count == 0)
            {
                throw new Exception(
                    $"Segment API returned no segments: {manifest?.Detail ?? content}");
            }

            await _repo.UpdateProgressAsync(jobId, "Mapping words to segments", 95, cancellationToken);

            await MapWordsToSegmentsAsync(movieId, manifest.Segments, cancellationToken);

            _logger.LogInformation(
                "Segmented movie {MovieId} into {Count} segments.",
                movieId, manifest.SegmentCount);
        }

        private async Task<int> GetWordCountAsync(int movieId, CancellationToken cancellationToken)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand(
                "SELECT COUNT(*) FROM frl.frl_transcript_words WHERE movieid = @movieid", conn);
            cmd.Parameters.AddWithValue("movieid", movieId);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            return Convert.ToInt32(result);
        }

        private async Task<bool> IsMovieSegmentedAsync(int movieId, CancellationToken cancellationToken)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand(
                "SELECT EXISTS(SELECT 1 FROM frl.frl_transcript_words WHERE movieid = @movieid AND segment_index IS NOT NULL)",
                conn);
            cmd.Parameters.AddWithValue("movieid", movieId);
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            return result is bool b && b;
        }

        private async Task<string?> GetR2KeyForMovieAsync(int movieId, CancellationToken cancellationToken)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);
            await using var cmd = new NpgsqlCommand(@"
SELECT r2_key FROM frl.frl_dialogue_transcription_jobs
WHERE movieid = @movieid AND r2_key IS NOT NULL
ORDER BY id DESC LIMIT 1;", conn);
            cmd.Parameters.AddWithValue("movieid", movieId);
            return await cmd.ExecuteScalarAsync(cancellationToken) as string;
        }

        /// <summary>
        /// Set segment_index and segment_start on each word based on the segment
        /// whose [start_time, start_time+duration) range contains the word's start.
        /// Uses a single UPDATE against a VALUES list of the segment ranges.
        /// </summary>
        private async Task MapWordsToSegmentsAsync(
            int movieId,
            List<SegmentManifestEntry> segments,
            CancellationToken cancellationToken)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            var sb = new System.Text.StringBuilder();
            sb.AppendLine("UPDATE frl.frl_transcript_words w");
            sb.AppendLine("SET segment_index = s.idx, segment_start = s.seg_start");
            sb.AppendLine("FROM (VALUES");

            var ordered = segments.OrderBy(s => s.Index).ToList();
            for (var i = 0; i < ordered.Count; i++)
            {
                var s = ordered[i];
                // The last segment absorbs anything past its start (guards against
                // rounding so no trailing word is left unmapped).
                var end = i == ordered.Count - 1
                    ? double.MaxValue
                    : ordered[i + 1].StartTime;
                var endLiteral = double.IsInfinity(end) || end == double.MaxValue
                    ? "1e18"
                    : end.ToString(System.Globalization.CultureInfo.InvariantCulture);
                sb.Append($"  ({s.Index}, {s.StartTime.ToString(System.Globalization.CultureInfo.InvariantCulture)}::float8, {endLiteral}::float8)");
                sb.AppendLine(i == ordered.Count - 1 ? "" : ",");
            }

            sb.AppendLine(") AS s(idx, seg_start, seg_end)");
            sb.AppendLine("WHERE w.movieid = @movieid");
            sb.AppendLine("  AND w.start_time >= s.seg_start");
            sb.AppendLine("  AND w.start_time < s.seg_end;");

            await using var cmd = new NpgsqlCommand(sb.ToString(), conn);
            cmd.CommandTimeout = 120;
            cmd.Parameters.AddWithValue("movieid", movieId);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
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

            // Bulk insert with binary COPY — streams all rows in a single
            // operation instead of one round-trip per word.
            await using (var writer = await conn.BeginBinaryImportAsync(
                "COPY frl.frl_transcript_words (movieid, word_index, word, start_time, end_time, confidence) FROM STDIN (FORMAT BINARY)",
                cancellationToken))
            {
                foreach (var w in words)
                {
                    var cleaned = CleanWord(w.Word);
                    if (string.IsNullOrEmpty(cleaned))
                        continue;

                    // Skip absurdly long tokens: real words are never this long,
                    // so these are transcription artifacts (run-on hallucinations),
                    // and they'd overflow the word/word_normalized VARCHAR(200) columns.
                    if (cleaned.Length > 100)
                        continue;

                    await writer.StartRowAsync(cancellationToken);
                    await writer.WriteAsync(movieId, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken);
                    await writer.WriteAsync(wordIndex, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken);
                    await writer.WriteAsync(cleaned, NpgsqlTypes.NpgsqlDbType.Varchar, cancellationToken);
                    await writer.WriteAsync(Math.Round(w.Start, 3), NpgsqlTypes.NpgsqlDbType.Double, cancellationToken);
                    await writer.WriteAsync(Math.Round(w.End, 3), NpgsqlTypes.NpgsqlDbType.Double, cancellationToken);

                    if (w.Probability.HasValue)
                        await writer.WriteAsync(Math.Round(w.Probability.Value, 4), NpgsqlTypes.NpgsqlDbType.Double, cancellationToken);
                    else
                        await writer.WriteNullAsync(cancellationToken);

                    wordIndex++;
                    insertedCount++;
                }

                await writer.CompleteAsync(cancellationToken);
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
