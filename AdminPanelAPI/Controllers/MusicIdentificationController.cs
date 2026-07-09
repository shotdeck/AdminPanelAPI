using AdminPanelAPI.Models;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MusicIdentificationController : ControllerBase
    {
        private readonly IMusicIdentificationJobRepository _jobRepository;
        private readonly IMusicJobQueue _jobQueue;
        private readonly IConfiguration _configuration;
        private readonly ILogger<MusicIdentificationController> _logger;
        private readonly string _connectionString;

        private readonly string _r2AccountId;
        private readonly string _r2AccessKey;
        private readonly string _r2SecretKey;
        private readonly string _r2BucketName;

        private const int PresignedUrlExpiryMinutes = 60;

        public MusicIdentificationController(
            IMusicIdentificationJobRepository jobRepository,
            IMusicJobQueue jobQueue,
            IConfiguration configuration,
            ILogger<MusicIdentificationController> logger)
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
        /// Queue a single movie for music identification.
        /// </summary>
        [HttpPost("identify/{movieId:int}")]
        public async Task<IActionResult> IdentifyMovie(
            int movieId,
            [FromQuery] string r2Key,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(r2Key))
                return BadRequest(new { error = "Query parameter 'r2Key' is required." });

            var jobId = await _jobRepository.CreateJobAsync(
                movieId, r2Key, null, cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                movieId,
                r2Key,
                status = "Queued"
            });
        }

        /// <summary>
        /// Get the status of a music identification job.
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
        /// Get the identified music segments for a movie (matched + unmatched).
        /// </summary>
        [HttpGet("segments/{movieId:int}")]
        public async Task<IActionResult> GetSegments(
            int movieId,
            CancellationToken cancellationToken)
        {
            var segments = await _jobRepository.GetSegmentsAsync(movieId, cancellationToken);

            return Ok(new
            {
                movieId,
                totalSegments = segments.Count,
                matchedCount = segments.Count(s => s.Matched),
                unmatchedCount = segments.Count(s => !s.Matched),
                segments
            });
        }

        /// <summary>
        /// Search identified music by song title or artist/band name. Returns
        /// matching songs, each grouped with every occurrence (movie + timestamps).
        /// </summary>
        [HttpGet("search")]
        public async Task<IActionResult> Search(
            [FromQuery] string q,
            [FromQuery] int limit = 500,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(q))
                return BadRequest(new { error = "Query parameter 'q' is required." });

            limit = Math.Clamp(limit, 1, 2000);
            var tracks = await _jobRepository.SearchTracksAsync(q.Trim(), limit, cancellationToken);

            return Ok(new
            {
                query = q.Trim(),
                trackCount = tracks.Count,
                occurrenceCount = tracks.Sum(t => t.OccurrenceCount),
                tracks
            });
        }

        /// <summary>
        /// Search movies by title (frl_movies.title), returning only movies that
        /// have identified music, with per-movie track/occurrence counts.
        /// </summary>
        [HttpGet("movies/search")]
        public async Task<IActionResult> SearchMovies(
            [FromQuery] string? q = null,
            [FromQuery] int limit = 200,
            CancellationToken cancellationToken = default)
        {
            // Empty query lists all processed movies (for populating a dropdown).
            var query = (q ?? string.Empty).Trim();
            limit = Math.Clamp(limit, 1, 1000);
            var movies = await _jobRepository.SearchMoviesByTitleAsync(query, limit, cancellationToken);

            return Ok(new
            {
                query,
                movieCount = movies.Count,
                movies
            });
        }

        /// <summary>
        /// List all identified songs for a movie, grouped with their occurrences.
        /// </summary>
        [HttpGet("movie/{movieId:int}/tracks")]
        public async Task<IActionResult> GetMovieTracks(
            int movieId,
            CancellationToken cancellationToken)
        {
            var tracks = await _jobRepository.GetMovieTracksAsync(movieId, cancellationToken);

            return Ok(new
            {
                movieId,
                trackCount = tracks.Count,
                occurrenceCount = tracks.Sum(t => t.OccurrenceCount),
                tracks
            });
        }

        /// <summary>
        /// Get a presigned R2 URL for streaming a movie file (from its music job).
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

            var r2Key = await GetR2KeyForMovieAsync(movieId, cancellationToken);
            if (r2Key == null)
                return NotFound(new { error = $"No music job found for movieId {movieId}." });

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

        private async Task<string?> GetR2KeyForMovieAsync(int movieId, CancellationToken cancellationToken)
        {
            const string sql = @"
SELECT r2_key
FROM frl.frl_join_movies_music_identification_jobs
WHERE movieid = @movieid
  AND r2_key IS NOT NULL
ORDER BY created_at DESC
LIMIT 1;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("movieid", movieId);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            return result as string;
        }
    }
}
