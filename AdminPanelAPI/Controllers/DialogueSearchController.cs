using AdminPanelAPI.Models;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Text;
using System.Text.RegularExpressions;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DialogueSearchController : ControllerBase
    {
        private readonly IDialogueTranscriptionJobRepository _jobRepository;
        private readonly IDialogueJobQueue _jobQueue;
        private readonly IConfiguration _configuration;
        private readonly ILogger<DialogueSearchController> _logger;
        private readonly string _connectionString;

        private readonly string _r2AccountId;
        private readonly string _r2AccessKey;
        private readonly string _r2SecretKey;
        private readonly string _r2BucketName;

        private const double PaddingSeconds = 0.5;
        private const double SpeechPaddingSeconds = 0.15;
        private const long MaxUploadSizeBytes = 10L * 1024 * 1024 * 1024; // 10 GB
        private const int PresignedUrlExpiryMinutes = 60;

        public DialogueSearchController(
            IDialogueTranscriptionJobRepository jobRepository,
            IDialogueJobQueue jobQueue,
            IConfiguration configuration,
            ILogger<DialogueSearchController> logger)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
            _configuration = configuration;
            _logger = logger;
            _connectionString = configuration.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing connection string: Default");

            _r2AccountId = configuration["R2:AccountId"] ?? "";
            _r2AccessKey = configuration["R2:AccessKey"] ?? "";
            _r2SecretKey = configuration["R2:SecretKey"] ?? "";
            _r2BucketName = configuration["DialogueSearch:R2BucketName"] ?? "movies";
        }

        /// <summary>
        /// Upload an MP4 file to R2 and queue it for dialogue transcription.
        /// </summary>
        [HttpPost("upload/{movieId:int}")]
        [RequestSizeLimit(MaxUploadSizeBytes)]
        [RequestFormLimits(MultipartBodyLengthLimit = MaxUploadSizeBytes)]
        public async Task<IActionResult> UploadAndTranscribe(
            int movieId,
            IFormFile file,
            CancellationToken cancellationToken = default)
        {
            return await UploadToR2AndQueueAsync(movieId, file, cancellationToken);
        }

        /// <summary>
        /// Upload an MP4 file to R2 and queue it for dialogue transcription,
        /// resolving the movie id from the file name. The file name is expected
        /// to contain the title and year, e.g. "The Boss Baby (2017)_BR.HD.01_SF.mp4",
        /// which is looked up in frl_movies (title + year) to find the idnum.
        /// </summary>
        [HttpPost("upload")]
        [RequestSizeLimit(MaxUploadSizeBytes)]
        [RequestFormLimits(MultipartBodyLengthLimit = MaxUploadSizeBytes)]
        public async Task<IActionResult> UploadAndTranscribeByFileName(
            IFormFile file,
            CancellationToken cancellationToken = default)
        {
            if (file == null || file.Length == 0)
                return BadRequest(new { error = "No file provided." });

            if (!TryParseTitleAndYear(file.FileName, out var title, out var year))
                return BadRequest(new
                {
                    error = $"Could not parse a title and year from file name '{file.FileName}'. " +
                            "Expected a name like 'The Boss Baby (2017)_BR.HD.01_SF.mp4'."
                });

            var movieId = await LookupMovieIdAsync(title, year, cancellationToken);
            if (movieId == null)
                return NotFound(new
                {
                    error = $"No movie found in frl_movies matching title '{title}' and year {year}."
                });

            return await UploadToR2AndQueueAsync(movieId.Value, file, cancellationToken);
        }

        private async Task<IActionResult> UploadToR2AndQueueAsync(
            int movieId,
            IFormFile file,
            CancellationToken cancellationToken)
        {
            if (file == null || file.Length == 0)
                return BadRequest(new { error = "No file provided." });

            if (!file.FileName.EndsWith(".mp4", StringComparison.OrdinalIgnoreCase))
                return BadRequest(new { error = "Only .mp4 files are accepted." });

            if (file.Length > MaxUploadSizeBytes)
                return BadRequest(new { error = $"File exceeds maximum size of {MaxUploadSizeBytes / (1024 * 1024 * 1024)} GB." });

            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey))
            {
                return StatusCode(500, new { error = "R2 credentials are not configured." });
            }

            var sanitizedName = SanitizeFileName(file.FileName);
            var r2Key = $"{movieId}/{sanitizedName}";

            var creds = new BasicAWSCredentials(_r2AccessKey.Trim(), _r2SecretKey.Trim());
            var s3Config = new AmazonS3Config
            {
                ServiceURL = $"https://{_r2AccountId.Trim()}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            using var s3Client = new AmazonS3Client(creds, s3Config);

            var alreadyExists = await R2ObjectExistsAsync(s3Client, r2Key, cancellationToken);

            if (alreadyExists)
            {
                _logger.LogInformation(
                    "File already exists in R2, skipping upload. MovieId={MovieId}, R2Key={R2Key}",
                    movieId, r2Key);
            }
            else
            {
                _logger.LogInformation(
                    "Uploading movie file to R2. MovieId={MovieId}, R2Key={R2Key}, Size={Size}",
                    movieId, r2Key, file.Length);

                await using var stream = file.OpenReadStream();
                var putRequest = new PutObjectRequest
                {
                    BucketName = _r2BucketName,
                    Key = r2Key,
                    InputStream = stream,
                    ContentType = "video/mp4",
                    DisablePayloadSigning = true
                };

                await s3Client.PutObjectAsync(putRequest, cancellationToken);

                _logger.LogInformation(
                    "Upload complete. MovieId={MovieId}, R2Key={R2Key}",
                    movieId, r2Key);
            }

            var jobId = await _jobRepository.CreateJobAsync(
                movieId, r2Key, null, cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                movieId,
                r2Key,
                r2Bucket = _r2BucketName,
                fileSizeBytes = file.Length,
                skippedUpload = alreadyExists,
                status = "Queued"
            });
        }

        /// <summary>
        /// Queue a single movie for dialogue transcription.
        /// </summary>
        [HttpPost("transcribe/{movieId:int}")]
        public async Task<IActionResult> TranscribeMovie(
            int movieId,
            [FromQuery] string? r2Key = null,
            [FromQuery] bool force = false,
            CancellationToken cancellationToken = default)
        {
            // force re-transcribes a movie that already has words (e.g. to purge
            // hallucinations after a transcription-settings change): delete its
            // words so the worker's "already has words -> skip" guard doesn't fire.
            // Segment files stay in R2; the job re-maps words to them afterwards.
            if (force)
                await _jobRepository.DeleteWordsAsync(movieId, cancellationToken);

            var jobId = await _jobRepository.CreateJobAsync(
                movieId, r2Key, null, cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                movieId,
                r2Key,
                status = "Queued",
                forced = force
            });
        }

        /// <summary>
        /// Queue a batch of untranscribed movies for dialogue transcription.
        /// </summary>
        [HttpPost("transcribe-batch")]
        public async Task<IActionResult> TranscribeBatch(
            [FromQuery] int count = 50,
            CancellationToken cancellationToken = default)
        {
            if (count <= 0)
                return BadRequest(new { error = "count must be greater than 0" });

            var movieIds = await _jobRepository.GetUntranscribedMovieIdsAsync(
                count, cancellationToken);

            var jobs = new List<object>();

            foreach (var movieId in movieIds)
            {
                var jobId = await _jobRepository.CreateJobAsync(
                    movieId, null, null, cancellationToken);

                await _jobQueue.QueueJobAsync(jobId, cancellationToken);

                jobs.Add(new { jobId, movieId, status = "Queued" });
            }

            return Ok(new
            {
                requested = count,
                queued = jobs.Count,
                jobs
            });
        }

        /// <summary>
        /// Get the status of a dialogue transcription job.
        /// </summary>
        [HttpGet("status/{jobId:long}")]
        public async Task<IActionResult> GetStatus(
            long jobId,
            CancellationToken cancellationToken)
        {
            var job = await _jobRepository.GetJobAsync(jobId, cancellationToken);

            if (job == null)
                return NotFound(new { message = $"Job {jobId} not found." });

            return Ok(job);
        }

        /// <summary>
        /// Get a presigned R2 URL for streaming a movie file.
        /// </summary>
        [HttpGet("video-url/{movieId:int}")]
        public async Task<IActionResult> GetVideoUrl(
            int movieId,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey))
            {
                return StatusCode(500, new { error = "R2 credentials are not configured." });
            }

            // Look up the R2 key from the most recent completed job for this movie
            var r2Key = await GetR2KeyForMovieAsync(movieId, cancellationToken);
            if (r2Key == null)
                return NotFound(new { error = $"No transcribed movie found for movieId {movieId}." });

            var creds = new BasicAWSCredentials(_r2AccessKey.Trim(), _r2SecretKey.Trim());
            var s3Config = new AmazonS3Config
            {
                ServiceURL = $"https://{_r2AccountId.Trim()}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            using var s3Client = new AmazonS3Client(creds, s3Config);

            var url = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
            {
                BucketName = _r2BucketName,
                Key = r2Key,
                Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                Verb = HttpVerb.GET
            });

            return Ok(new { movieId, r2Key, url, expiresInMinutes = PresignedUrlExpiryMinutes });
        }

        /// <summary>
        /// Get a presigned R2 URL for a single ~10s movie segment. The player
        /// loads this small file and seeks within it instead of the full movie.
        /// </summary>
        [HttpGet("segment-url/{movieId:int}")]
        public IActionResult GetSegmentUrl(
            int movieId,
            [FromQuery] int index)
        {
            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey))
            {
                return StatusCode(500, new { error = "R2 credentials are not configured." });
            }

            if (index < 0)
                return BadRequest(new { error = "index must be >= 0" });

            var key = $"segments/{movieId}/{index:D6}.mp4";

            var creds = new BasicAWSCredentials(_r2AccessKey.Trim(), _r2SecretKey.Trim());
            var s3Config = new AmazonS3Config
            {
                ServiceURL = $"https://{_r2AccountId.Trim()}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            using var s3Client = new AmazonS3Client(creds, s3Config);

            var url = s3Client.GetPreSignedURL(new GetPreSignedUrlRequest
            {
                BucketName = _r2BucketName,
                Key = key,
                Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                Verb = HttpVerb.GET
            });

            return Ok(new { movieId, index, r2Key = key, url, expiresInMinutes = PresignedUrlExpiryMinutes });
        }

        /// <summary>
        /// Split a single already-uploaded movie into segments (no re-transcription
        /// if it already has words). Reuses the transcription job pipeline.
        /// </summary>
        [HttpPost("segment/{movieId:int}")]
        public async Task<IActionResult> SegmentMovie(
            int movieId,
            [FromQuery] string? r2Key = null,
            [FromQuery] bool force = false,
            CancellationToken cancellationToken = default)
        {
            // force re-segments a movie that is already segmented (e.g. to fix bad
            // offsets): clear its segment mapping so the job re-splits + re-maps.
            if (force)
                await _jobRepository.ClearSegmentMappingAsync(movieId, cancellationToken);

            var jobId = await _jobRepository.CreateJobAsync(
                movieId, r2Key, null, cancellationToken);
            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new { jobId, movieId, status = "Queued", forced = force });
        }

        /// <summary>
        /// Backfill: queue segmenting for a batch of movies that have already been
        /// transcribed but not yet split into segments. Skips re-transcription.
        /// With force=true, re-segments movies that are ALREADY segmented (clears
        /// their existing mapping first) — used to repair bad segment offsets.
        /// </summary>
        [HttpPost("segment-batch")]
        public async Task<IActionResult> SegmentBatch(
            [FromQuery] int count = 25,
            [FromQuery] bool force = false,
            CancellationToken cancellationToken = default)
        {
            if (count <= 0)
                return BadRequest(new { error = "count must be greater than 0" });

            var movieIds = force
                ? await _jobRepository.GetSegmentedMovieIdsAsync(count, cancellationToken)
                : await _jobRepository.GetMovieIdsNeedingSegmentsAsync(count, cancellationToken);

            var jobs = new List<object>();
            foreach (var movieId in movieIds)
            {
                if (force)
                    await _jobRepository.ClearSegmentMappingAsync(movieId, cancellationToken);

                var jobId = await _jobRepository.CreateJobAsync(
                    movieId, null, null, cancellationToken);
                await _jobQueue.QueueJobAsync(jobId, cancellationToken);
                jobs.Add(new { jobId, movieId, status = "Queued" });
            }

            return Ok(new { requested = count, queued = jobs.Count, forced = force, jobs });
        }

        /// <summary>
        /// Search for a word or phrase across all transcribed movies.
        /// Returns matching clips with timestamps.
        /// </summary>
        [HttpGet("search")]
        public async Task<IActionResult> Search(
            [FromQuery] string q,
            [FromQuery] int limit = 50,
            [FromQuery] int? movieId = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(q))
                return BadRequest(new { error = "Query parameter 'q' is required." });

            var words = TokenizeQuery(q);
            if (words.Count == 0)
                return BadRequest(new { error = "Query is empty after tokenization." });

            limit = Math.Clamp(limit, 1, 200);

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            List<DialogueSearchResult> results;

            if (words.Count == 1)
            {
                results = await SearchSingleWord(conn, words[0], limit, movieId, cancellationToken);
            }
            else
            {
                results = await SearchPhrase(conn, words, limit, movieId, cancellationToken);
            }

            return Ok(new DialogueSearchResponse
            {
                Query = q,
                TotalResults = results.Count,
                Results = results
            });
        }

        /// <summary>
        /// Speech mode search: breaks input text into optimal phrase segments
        /// and finds a matching clip for each segment. Returns clips with small
        /// padding (±0.15s) clamped to not overlap neighboring words.
        /// </summary>
        [HttpGet("speech-search")]
        public async Task<IActionResult> SpeechSearch(
            [FromQuery] string text,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(text))
                return BadRequest(new { error = "Query parameter 'text' is required." });

            var words = TokenizeQuery(text);
            if (words.Count == 0)
                return BadRequest(new { error = "Text is empty after tokenization." });

            const int maxPhraseLength = 8;

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            var segments = new List<DialogueSearchResult>();
            var position = 0;

            while (position < words.Count)
            {
                DialogueSearchResult? bestMatch = null;
                var maxLen = Math.Min(maxPhraseLength, words.Count - position);

                // Try longest phrase first, then shorter
                for (var len = maxLen; len >= 1; len--)
                {
                    var phraseWords = words.GetRange(position, len);
                    var match = await FindFirstMatch(conn, phraseWords, cancellationToken);

                    if (match != null)
                    {
                        bestMatch = match;
                        position += len;
                        break;
                    }
                }

                if (bestMatch != null)
                {
                    segments.Add(bestMatch);
                }
                else
                {
                    // No match found for this word — skip it
                    segments.Add(new DialogueSearchResult
                    {
                        MovieId = 0,
                        Phrase = words[position],
                        Context = "",
                        StartTime = 0,
                        EndTime = 0
                    });
                    position++;
                }
            }

            return Ok(new
            {
                text,
                totalSegments = segments.Count,
                segments
            });
        }

        /// <summary>
        /// Find the first matching clip for a phrase. Applies small padding
        /// clamped to not bleed into neighboring words.
        /// </summary>
        private async Task<DialogueSearchResult?> FindFirstMatch(
            NpgsqlConnection conn,
            List<string> phraseWords,
            CancellationToken cancellationToken)
        {
            if (phraseWords.Count == 1)
            {
                const string sql = @"
SELECT w.movieid, w.word, w.start_time, w.end_time, w.word_index,
       (SELECT end_time FROM frl.frl_transcript_words
        WHERE movieid = w.movieid AND word_index = w.word_index - 1) as prev_end,
       (SELECT start_time FROM frl.frl_transcript_words
        WHERE movieid = w.movieid AND word_index = w.word_index + 1) as next_start
FROM frl.frl_transcript_words w
WHERE w.word_normalized = @word
ORDER BY RANDOM()
LIMIT 1;";

                await using var cmd = new NpgsqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("word", phraseWords[0]);

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    var rawStart = reader.GetDouble(2);
                    var rawEnd = reader.GetDouble(3);
                    var prevEnd = reader.IsDBNull(5) ? (double?)null : reader.GetDouble(5);
                    var nextStart = reader.IsDBNull(6) ? (double?)null : reader.GetDouble(6);

                    var result = new DialogueSearchResult
                    {
                        MovieId = reader.GetInt32(0),
                        Phrase = reader.GetString(1),
                        Context = "",
                        StartTime = ClampPaddedStart(rawStart, prevEnd),
                        EndTime = ClampPaddedEnd(rawEnd, nextStart)
                    };
                    await reader.CloseAsync();

                    result.Context = await GetContextAsync(
                        conn, result.MovieId, result.StartTime, result.EndTime, cancellationToken);

                    return result;
                }
                await reader.CloseAsync();
                return null;
            }

            // Multi-word phrase: self-join query with neighbor boundaries
            var lastIdx = phraseWords.Count - 1;
            var sb = new StringBuilder();
            sb.AppendLine($"SELECT w0.movieid, w0.start_time, w{lastIdx}.end_time, w0.word_index, w{lastIdx}.word_index,");
            sb.AppendLine($"  (SELECT end_time FROM frl.frl_transcript_words WHERE movieid = w0.movieid AND word_index = w0.word_index - 1) as prev_end,");
            sb.AppendLine($"  (SELECT start_time FROM frl.frl_transcript_words WHERE movieid = w0.movieid AND word_index = w{lastIdx}.word_index + 1) as next_start");
            sb.AppendLine("FROM frl.frl_transcript_words w0");

            for (var i = 1; i < phraseWords.Count; i++)
            {
                sb.AppendLine($"JOIN frl.frl_transcript_words w{i}");
                sb.AppendLine($"  ON w{i}.movieid = w0.movieid");
                sb.AppendLine($"  AND w{i}.word_index = w0.word_index + {i}");
                sb.AppendLine($"  AND w{i}.word_normalized = @word{i}");
            }

            sb.AppendLine("WHERE w0.word_normalized = @word0");
            sb.AppendLine("ORDER BY RANDOM()");
            sb.AppendLine("LIMIT 1;");

            await using var phraseCmd = new NpgsqlCommand(sb.ToString(), conn);
            for (var i = 0; i < phraseWords.Count; i++)
            {
                phraseCmd.Parameters.AddWithValue($"word{i}", phraseWords[i]);
            }

            await using var phraseReader = await phraseCmd.ExecuteReaderAsync(cancellationToken);
            if (await phraseReader.ReadAsync(cancellationToken))
            {
                var rawStart = phraseReader.GetDouble(1);
                var rawEnd = phraseReader.GetDouble(2);
                var prevEnd = phraseReader.IsDBNull(5) ? (double?)null : phraseReader.GetDouble(5);
                var nextStart = phraseReader.IsDBNull(6) ? (double?)null : phraseReader.GetDouble(6);

                var result = new DialogueSearchResult
                {
                    MovieId = phraseReader.GetInt32(0),
                    Phrase = string.Join(" ", phraseWords),
                    Context = "",
                    StartTime = ClampPaddedStart(rawStart, prevEnd),
                    EndTime = ClampPaddedEnd(rawEnd, nextStart)
                };
                await phraseReader.CloseAsync();

                result.Context = await GetContextAsync(
                    conn, result.MovieId, result.StartTime, result.EndTime, cancellationToken);

                return result;
            }
            await phraseReader.CloseAsync();
            return null;
        }

        /// <summary>
        /// Apply speech padding to start time, clamped so it doesn't bleed
        /// into the previous word.
        /// </summary>
        private static double ClampPaddedStart(double startTime, double? prevWordEnd)
        {
            var padded = startTime - SpeechPaddingSeconds;
            if (prevWordEnd.HasValue)
                padded = Math.Max(padded, prevWordEnd.Value);
            return Math.Max(0, padded);
        }

        /// <summary>
        /// Apply speech padding to end time, clamped so it doesn't bleed
        /// into the next word.
        /// </summary>
        private static double ClampPaddedEnd(double endTime, double? nextWordStart)
        {
            var padded = endTime + SpeechPaddingSeconds;
            if (nextWordStart.HasValue)
                padded = Math.Min(padded, nextWordStart.Value);
            return padded;
        }

        private async Task<List<DialogueSearchResult>> SearchSingleWord(
            NpgsqlConnection conn,
            string word,
            int limit,
            int? movieId,
            CancellationToken cancellationToken)
        {
            var sql = @"
SELECT w.movieid, m.title, w.word, w.start_time, w.end_time, w.word_index,
       w.segment_index, w.segment_start
FROM frl.frl_transcript_words w
LEFT JOIN frl.frl_movies m ON m.idnum = w.movieid
WHERE w.word_normalized = @word"
                + (movieId.HasValue ? "\n  AND w.movieid = @movieId" : "")
                + @"
ORDER BY RANDOM()
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("word", word);
            cmd.Parameters.AddWithValue("limit", limit);
            if (movieId.HasValue)
                cmd.Parameters.AddWithValue("movieId", movieId.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var results = new List<DialogueSearchResult>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var rowMovieId = reader.GetInt32(0);
                var movieTitle = reader.IsDBNull(1) ? null : reader.GetString(1);
                var matchedWord = reader.GetString(2);
                var startTime = reader.GetDouble(3);
                var endTime = reader.GetDouble(4);
                var wordIndex = reader.GetInt32(5);

                results.Add(new DialogueSearchResult
                {
                    MovieId = rowMovieId,
                    MovieTitle = movieTitle,
                    Phrase = matchedWord,
                    Context = "",
                    StartTime = Math.Max(0, startTime - PaddingSeconds),
                    EndTime = endTime + PaddingSeconds,
                    SegmentIndex = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                    SegmentStart = reader.IsDBNull(7) ? null : reader.GetDouble(7)
                });
            }

            await reader.CloseAsync();

            // Fill in context for each result
            foreach (var result in results)
            {
                result.Context = await GetContextAsync(
                    conn, result.MovieId, result.StartTime, result.EndTime, cancellationToken);
            }

            return results;
        }

        private async Task<List<DialogueSearchResult>> SearchPhrase(
            NpgsqlConnection conn,
            List<string> words,
            int limit,
            int? movieId,
            CancellationToken cancellationToken)
        {
            // Build a query with self-joins for consecutive word matching
            var sb = new StringBuilder();
            sb.AppendLine("SELECT w0.movieid, m.title, w0.word_index, w0.start_time,");
            sb.AppendLine($"  w{words.Count - 1}.end_time, w0.segment_index, w0.segment_start");
            sb.AppendLine("FROM frl.frl_transcript_words w0");

            for (var i = 1; i < words.Count; i++)
            {
                sb.AppendLine($"JOIN frl.frl_transcript_words w{i}");
                sb.AppendLine($"  ON w{i}.movieid = w0.movieid");
                sb.AppendLine($"  AND w{i}.word_index = w0.word_index + {i}");
                sb.AppendLine($"  AND w{i}.word_normalized = @word{i}");
            }

            sb.AppendLine("LEFT JOIN frl.frl_movies m ON m.idnum = w0.movieid");
            sb.AppendLine("WHERE w0.word_normalized = @word0");
            if (movieId.HasValue)
                sb.AppendLine("  AND w0.movieid = @movieId");
            sb.AppendLine("ORDER BY RANDOM()");
            sb.AppendLine("LIMIT @limit;");

            await using var cmd = new NpgsqlCommand(sb.ToString(), conn);

            for (var i = 0; i < words.Count; i++)
            {
                cmd.Parameters.AddWithValue($"word{i}", words[i]);
            }
            cmd.Parameters.AddWithValue("limit", limit);
            if (movieId.HasValue)
                cmd.Parameters.AddWithValue("movieId", movieId.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var results = new List<DialogueSearchResult>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var rowMovieId = reader.GetInt32(0);
                var movieTitle = reader.IsDBNull(1) ? null : reader.GetString(1);
                var startTime = reader.GetDouble(3);
                var endTime = reader.GetDouble(4);

                results.Add(new DialogueSearchResult
                {
                    MovieId = rowMovieId,
                    MovieTitle = movieTitle,
                    Phrase = string.Join(" ", words),
                    Context = "",
                    StartTime = Math.Max(0, startTime - PaddingSeconds),
                    EndTime = endTime + PaddingSeconds,
                    SegmentIndex = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    SegmentStart = reader.IsDBNull(6) ? null : reader.GetDouble(6)
                });
            }

            await reader.CloseAsync();

            foreach (var result in results)
            {
                result.Context = await GetContextAsync(
                    conn, result.MovieId, result.StartTime, result.EndTime, cancellationToken);
            }

            return results;
        }

        private static async Task<string> GetContextAsync(
            NpgsqlConnection conn,
            int movieId,
            double startTime,
            double endTime,
            CancellationToken cancellationToken)
        {
            // Get surrounding words for context (±3 seconds)
            const string sql = @"
SELECT word
FROM frl.frl_transcript_words
WHERE movieid = @movieid
  AND start_time >= @ctx_start
  AND end_time <= @ctx_end
ORDER BY word_index
LIMIT 30;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("movieid", movieId);
            cmd.Parameters.AddWithValue("ctx_start", Math.Max(0, startTime - 3));
            cmd.Parameters.AddWithValue("ctx_end", endTime + 3);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var contextWords = new List<string>();
            while (await reader.ReadAsync(cancellationToken))
            {
                contextWords.Add(reader.GetString(0));
            }

            return string.Join(" ", contextWords);
        }

        private static List<string> TokenizeQuery(string query)
        {
            // Split on whitespace/punctuation, then strip every non-alphanumeric
            // char from each token so it matches word_normalized (which is
            // regexp_replace(lower(word), '[^a-z0-9]', '')). This makes matching
            // apostrophe/punctuation- and case-insensitive: "lets", "let's" and
            // "Let's" all normalize to "lets".
            return Regex.Replace(query.ToLowerInvariant(), @"[^\w']", " ")
                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Select(w => Regex.Replace(w, @"[^a-z0-9]", ""))
                .Where(w => !string.IsNullOrEmpty(w))
                .ToList();
        }

        private async Task<string?> GetR2KeyForMovieAsync(
            int movieId,
            CancellationToken cancellationToken)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            const string sql = @"
SELECT r2_key FROM frl.frl_dialogue_transcription_jobs
WHERE movieid = @movieId AND r2_key IS NOT NULL
ORDER BY id DESC LIMIT 1;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("movieId", movieId);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            return result as string;
        }

        private async Task<bool> R2ObjectExistsAsync(
            AmazonS3Client client,
            string key,
            CancellationToken cancellationToken)
        {
            try
            {
                await client.GetObjectMetadataAsync(
                    _r2BucketName, key, cancellationToken);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        private static string SanitizeFileName(string fileName)
        {
            var name = Path.GetFileName(fileName);
            return Regex.Replace(name, @"[^\w.\-]", "_");
        }

        private static readonly Regex TitleYearRegex =
            new(@"^(?<title>.+?)\s*\((?<year>\d{4})\)", RegexOptions.Compiled);

        /// <summary>
        /// Parse the movie title and release year from a file name such as
        /// "The Boss Baby (2017)_BR.HD.01_SF.mp4".
        /// </summary>
        private static bool TryParseTitleAndYear(string fileName, out string title, out int year)
        {
            title = "";
            year = 0;

            var name = Path.GetFileNameWithoutExtension(fileName);
            var match = TitleYearRegex.Match(name);
            if (!match.Success)
                return false;

            title = match.Groups["title"].Value.Trim();
            year = int.Parse(match.Groups["year"].Value);
            return !string.IsNullOrWhiteSpace(title);
        }

        private async Task<int?> LookupMovieIdAsync(
            string title,
            int year,
            CancellationToken cancellationToken)
        {
            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            const string sql = @"
SELECT idnum FROM frl.frl_movies
WHERE LOWER(title) = LOWER(@title) AND year = @year
ORDER BY idnum
LIMIT 1;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("title", title);
            cmd.Parameters.AddWithValue("year", year);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result == null || result is DBNull)
                return null;

            return Convert.ToInt32(result);
        }
    }
}
