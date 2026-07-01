using AdminPanelAPI.Models;
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
        private readonly string _connectionString;

        private const double PaddingSeconds = 0.5;

        public DialogueSearchController(
            IDialogueTranscriptionJobRepository jobRepository,
            IDialogueJobQueue jobQueue,
            IConfiguration configuration)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
            _configuration = configuration;
            _connectionString = configuration.GetConnectionString("Default")
                ?? throw new InvalidOperationException("Missing connection string: Default");
        }

        /// <summary>
        /// Queue a single movie for dialogue transcription.
        /// </summary>
        [HttpPost("transcribe/{movieId:int}")]
        public async Task<IActionResult> TranscribeMovie(
            int movieId,
            CancellationToken cancellationToken)
        {
            var jobId = await _jobRepository.CreateJobAsync(
                movieId, null, null, cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                movieId,
                status = "Queued"
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
        /// Search for a word or phrase across all transcribed movies.
        /// Returns matching clips with timestamps.
        /// </summary>
        [HttpGet("search")]
        public async Task<IActionResult> Search(
            [FromQuery] string q,
            [FromQuery] int limit = 50,
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
                results = await SearchSingleWord(conn, words[0], limit, cancellationToken);
            }
            else
            {
                results = await SearchPhrase(conn, words, limit, cancellationToken);
            }

            return Ok(new DialogueSearchResponse
            {
                Query = q,
                TotalResults = results.Count,
                Results = results
            });
        }

        private async Task<List<DialogueSearchResult>> SearchSingleWord(
            NpgsqlConnection conn,
            string word,
            int limit,
            CancellationToken cancellationToken)
        {
            const string sql = @"
SELECT w.movieid, w.word, w.start_time, w.end_time, w.word_index
FROM frl.frl_transcript_words w
WHERE w.word = @word
ORDER BY w.movieid, w.start_time
LIMIT @limit;";

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("word", word);
            cmd.Parameters.AddWithValue("limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var results = new List<DialogueSearchResult>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var movieId = reader.GetInt32(0);
                var matchedWord = reader.GetString(1);
                var startTime = reader.GetDouble(2);
                var endTime = reader.GetDouble(3);
                var wordIndex = reader.GetInt32(4);

                results.Add(new DialogueSearchResult
                {
                    MovieId = movieId,
                    Phrase = matchedWord,
                    Context = "",
                    StartTime = Math.Max(0, startTime - PaddingSeconds),
                    EndTime = endTime + PaddingSeconds
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
            CancellationToken cancellationToken)
        {
            // Build a query with self-joins for consecutive word matching
            var sb = new StringBuilder();
            sb.AppendLine("SELECT w0.movieid, w0.word_index, w0.start_time,");
            sb.AppendLine($"  w{words.Count - 1}.end_time");
            sb.AppendLine("FROM frl.frl_transcript_words w0");

            for (var i = 1; i < words.Count; i++)
            {
                sb.AppendLine($"JOIN frl.frl_transcript_words w{i}");
                sb.AppendLine($"  ON w{i}.movieid = w0.movieid");
                sb.AppendLine($"  AND w{i}.word_index = w0.word_index + {i}");
                sb.AppendLine($"  AND w{i}.word = @word{i}");
            }

            sb.AppendLine("WHERE w0.word = @word0");
            sb.AppendLine("ORDER BY w0.movieid, w0.start_time");
            sb.AppendLine("LIMIT @limit;");

            await using var cmd = new NpgsqlCommand(sb.ToString(), conn);

            for (var i = 0; i < words.Count; i++)
            {
                cmd.Parameters.AddWithValue($"word{i}", words[i]);
            }
            cmd.Parameters.AddWithValue("limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            var results = new List<DialogueSearchResult>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var movieId = reader.GetInt32(0);
                var startTime = reader.GetDouble(2);
                var endTime = reader.GetDouble(3);

                results.Add(new DialogueSearchResult
                {
                    MovieId = movieId,
                    Phrase = string.Join(" ", words),
                    Context = "",
                    StartTime = Math.Max(0, startTime - PaddingSeconds),
                    EndTime = endTime + PaddingSeconds
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
            return Regex.Replace(query.ToLowerInvariant(), @"[^\w']", " ")
                .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                .Select(w => w.Trim('\''))
                .Where(w => !string.IsNullOrEmpty(w))
                .ToList();
        }
    }
}
