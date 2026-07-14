using AdminPanelAPI.Models;
using AdminPanelAPI.Services;
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
        private readonly ISoundtrackReconciliationService _reconciliationService;
        private readonly IStreamingLinkService _streamingLinkService;
        private readonly ITrackDetailsService _trackDetailsService;
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
            ISoundtrackReconciliationService reconciliationService,
            IStreamingLinkService streamingLinkService,
            ITrackDetailsService trackDetailsService,
            IConfiguration configuration,
            ILogger<MusicIdentificationController> logger)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
            _reconciliationService = reconciliationService;
            _streamingLinkService = streamingLinkService;
            _trackDetailsService = trackDetailsService;
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
        /// Queue a single movie for music identification. If 'r2Key' is omitted it
        /// is resolved from the movies bucket by convention (movies/{movieId}/*.mp4).
        /// </summary>
        [HttpPost("identify/{movieId:int}")]
        public async Task<IActionResult> IdentifyMovie(
            int movieId,
            [FromQuery] string? r2Key = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(r2Key))
            {
                // Every movie is dialogue-processed before music, so its r2_key is
                // already recorded on the dialogue job — reuse it (no R2 call). Fall
                // back to listing the movies bucket by convention if there's none.
                r2Key = await GetR2KeyFromDialogueJobAsync(movieId, cancellationToken)
                    ?? await ResolveR2KeyForMovieAsync(movieId, cancellationToken);
                if (string.IsNullOrWhiteSpace(r2Key))
                    return NotFound(new
                    {
                        error = $"No r2_key found for movie {movieId} (no dialogue job and "
                            + $"no .mp4 under '{movieId}/'); pass 'r2Key' explicitly."
                    });
            }

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
            [FromQuery] bool includeRejected = false,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(q))
                return BadRequest(new { error = "Query parameter 'q' is required." });

            limit = Math.Clamp(limit, 1, 2000);
            var tracks = await _jobRepository.SearchTracksAsync(q.Trim(), limit, includeRejected, cancellationToken);

            return Ok(new
            {
                query = q.Trim(),
                trackCount = tracks.Count,
                occurrenceCount = tracks.Sum(t => t.OccurrenceCount),
                tracks
            });
        }

        /// <summary>
        /// List distinct artists and song titles that have identified music,
        /// for populating the Band/Song search dropdown. Empty query returns all.
        /// </summary>
        [HttpGet("search/options")]
        public async Task<IActionResult> SearchOptions(
            [FromQuery] string? q = null,
            [FromQuery] int limit = 1000,
            CancellationToken cancellationToken = default)
        {
            var query = (q ?? string.Empty).Trim();
            limit = Math.Clamp(limit, 1, 5000);
            var options = await _jobRepository.GetSearchOptionsAsync(query, limit, cancellationToken);

            return Ok(new
            {
                query,
                artistCount = options.Artists.Count,
                songCount = options.Songs.Count,
                artists = options.Artists,
                songs = options.Songs
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
            [FromQuery] bool includeRejected = false,
            CancellationToken cancellationToken = default)
        {
            var tracks = await _jobRepository.GetMovieTracksAsync(movieId, includeRejected, cancellationToken);
            var soundtrack = await _jobRepository.GetMovieSoundtrackAsync(movieId, cancellationToken);

            return Ok(new
            {
                movieId,
                trackCount = tracks.Count,
                occurrenceCount = tracks.Sum(t => t.OccurrenceCount),
                soundtrack,
                tracks
            });
        }

        /// <summary>
        /// Basic movie metadata (title, year, poster) by id. Available as soon
        /// as the movie exists, so the upload UI can show the poster before
        /// music identification finishes.
        /// </summary>
        [HttpGet("movie/{movieId:int}/info")]
        public async Task<IActionResult> GetMovieInfo(
            int movieId,
            CancellationToken cancellationToken = default)
        {
            var info = await _jobRepository.GetMovieInfoAsync(movieId, cancellationToken);
            if (info == null)
                return NotFound(new { message = $"Movie {movieId} not found." });

            return Ok(info);
        }

        /// <summary>
        /// Rich metadata for a single identified track (description, writers,
        /// composers, producers, album/release info). Fetched from public
        /// sources on first request and cached; pass refresh=true to re-fetch.
        /// </summary>
        [HttpGet("song/{songId:long}/details")]
        public async Task<IActionResult> GetTrackDetails(
            long songId,
            [FromQuery] int? movieId,
            [FromQuery] bool refresh,
            CancellationToken cancellationToken)
        {
            var details = await _trackDetailsService.GetOrFetchAsync(songId, movieId, refresh, cancellationToken);
            if (details == null)
                return NotFound();

            // If the track looks like a false-positive match (agent says it's not
            // in the film, or it was released after the film), flag it for review.
            // Otherwise, if the AI confirms it's in the film and the fingerprint
            // match is solid, promote it from unverified to confirmed — the
            // soundtrack cross-check often can't corroborate real needle-drops
            // and score cues, so the AI verdict is a second way to confirm.
            if (movieId.HasValue)
            {
                if (TrackDetailsService.ShouldFlagForReview(details))
                {
                    await _jobRepository.SetSongConfidenceAsync(
                        movieId.Value,
                        new Dictionary<long, string> { [songId] = "review" },
                        cancellationToken);
                }
                else if (TrackDetailsService.AiConfirmsInFilm(details) &&
                         await _jobRepository.GetSongMaxScoreAsync(
                             movieId.Value, songId, cancellationToken)
                         >= TrackDetailsService.ConfirmScoreThreshold)
                {
                    await _jobRepository.PromoteUnverifiedToConfirmedAsync(
                        movieId.Value, songId, cancellationToken);
                }
                else
                {
                    // Not a false positive and not strong enough to auto-confirm:
                    // ensure an unreconciled track still gets a visible status.
                    await _jobRepository.BaselineNullToUnverifiedAsync(
                        movieId.Value, songId, cancellationToken);
                }
            }

            return Ok(details);
        }

        /// <summary>
        /// Save a human-authored description for a track in a specific movie and
        /// lock it, so AI regeneration/backfill never overwrites it.
        /// </summary>
        [HttpPut("song/{songId:long}/description")]
        public async Task<IActionResult> SaveTrackDescription(
            long songId,
            [FromBody] SaveDescriptionRequest request,
            CancellationToken cancellationToken)
        {
            if (request == null || request.MovieId <= 0)
                return BadRequest("movieId is required.");
            if (string.IsNullOrWhiteSpace(request.Description))
                return BadRequest("description is required.");

            var details = await _trackDetailsService.SaveDescriptionAsync(
                songId, request.MovieId, request.Description, cancellationToken);
            if (details == null)
                return NotFound();
            return Ok(details);
        }

        /// <summary>
        /// Revert a track's description in a movie back to AI-generated (drops
        /// the manual/cached row and regenerates).
        /// </summary>
        [HttpDelete("song/{songId:long}/description")]
        public async Task<IActionResult> RevertTrackDescription(
            long songId,
            [FromQuery] int movieId,
            CancellationToken cancellationToken)
        {
            if (movieId <= 0)
                return BadRequest("movieId is required.");

            var details = await _trackDetailsService.RevertDescriptionAsync(
                songId, movieId, cancellationToken);
            if (details == null)
                return NotFound();
            return Ok(details);
        }

        /// <summary>
        /// Reconcile a movie's identified tracks against its known soundtrack
        /// (Wikipedia): tags each occurrence confirmed / review / unverified and
        /// returns the report. Non-destructive — nothing is deleted.
        /// </summary>
        [HttpPost("reconcile/{movieId:int}")]
        public async Task<IActionResult> Reconcile(
            int movieId,
            CancellationToken cancellationToken)
        {
            var result = await _reconciliationService.ReconcileAsync(movieId, cancellationToken);
            return Ok(result);
        }

        /// <summary>
        /// Set the confidence status of a track's occurrences in a movie
        /// (confirmed / review / unverified / rejected). Rejected tracks are
        /// excluded from search and movie-track results.
        /// </summary>
        [HttpPut("song/{songId:long}/status")]
        public async Task<IActionResult> SetTrackStatus(
            long songId,
            [FromBody] SetStatusRequest request,
            CancellationToken cancellationToken)
        {
            if (request == null || request.MovieId <= 0)
                return BadRequest(new { error = "movieId is required." });

            var status = (request.Status ?? string.Empty).Trim().ToLowerInvariant();
            var allowed = new[] { "confirmed", "review", "unverified", "rejected" };
            if (!allowed.Contains(status))
                return BadRequest(new
                {
                    error = "status must be one of: confirmed, review, unverified, rejected."
                });

            await _jobRepository.SetSongConfidenceAsync(
                request.MovieId,
                new Dictionary<long, string> { [songId] = status },
                cancellationToken);

            return Ok(new { songId, movieId = request.MovieId, status });
        }

        /// <summary>
        /// Edit a track's title and artist (admin). A blank artist clears it;
        /// a non-blank artist is found-or-created so a brand-new or existing
        /// name can be used.
        /// </summary>
        [HttpPut("song/{songId:long}/track")]
        public async Task<IActionResult> UpdateTrack(
            long songId,
            [FromBody] UpdateTrackRequest request,
            CancellationToken cancellationToken)
        {
            var title = (request?.Title ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(title))
                return BadRequest(new { error = "title is required." });

            var updated = await _jobRepository.UpdateSongTrackAsync(
                songId, title, request?.Artist, cancellationToken);
            if (!updated)
                return NotFound();

            var artist = string.IsNullOrWhiteSpace(request?.Artist)
                ? null
                : request!.Artist!.Trim();
            return Ok(new { songId, title, artist });
        }

        /// <summary>
        /// Backfill streaming links on a movie's identified tracks: search Spotify
        /// for each (title, artist) with a similarity guard, store the matched
        /// Spotify URL, and derive a universal all-services link (song.link).
        /// Non-destructive — only fills links. Pass force=true to re-resolve
        /// tracks that already have links.
        /// </summary>
        [HttpPost("streaming-links/{movieId:int}")]
        public async Task<IActionResult> BackfillStreamingLinks(
            int movieId,
            [FromQuery] bool force = false,
            CancellationToken cancellationToken = default)
        {
            var result = await _streamingLinkService.BackfillAsync(movieId, force, cancellationToken);
            if (!result.CredentialsConfigured)
                return StatusCode(503, new { error = "Spotify credentials not configured on the server." });
            return Ok(result);
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

        /// <summary>
        /// Return the movie's playback segment manifest (distinct segment index +
        /// its real start offset), derived from the dialogue transcript. Used by
        /// the frontend to map a music clip's time to the segment file that the
        /// /export endpoint cuts from. Every movie is dialogue-processed, so the
        /// segments exist for all movies that have identified music.
        /// </summary>
        [HttpGet("video-segments/{movieId:int}")]
        public async Task<IActionResult> GetVideoSegments(
            int movieId,
            CancellationToken cancellationToken)
        {
            const string sql = @"
SELECT DISTINCT segment_index, segment_start
FROM frl.frl_transcript_words
WHERE movieid = @movieid
  AND segment_index IS NOT NULL
  AND segment_start IS NOT NULL
ORDER BY segment_index;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("movieid", movieId);

            var segments = new List<VideoSegment>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                segments.Add(new VideoSegment
                {
                    Index = reader.GetInt32(0),
                    Start = reader.GetDouble(1)
                });
            }

            return Ok(new { movieId, segments });
        }

        /// <summary>
        /// Resolve a movie's r2_key from its most recent dialogue transcription job.
        /// Every movie is dialogue-processed before music, so this is the cheapest,
        /// most authoritative source. Returns null if the movie has no dialogue job.
        /// </summary>
        private async Task<string?> GetR2KeyFromDialogueJobAsync(
            int movieId, CancellationToken cancellationToken)
        {
            const string sql = @"
SELECT r2_key
FROM frl.frl_dialogue_transcription_jobs
WHERE movieid = @movieid
  AND r2_key IS NOT NULL
ORDER BY id DESC
LIMIT 1;";

            await using var conn = new NpgsqlConnection(_connectionString);
            await conn.OpenAsync(cancellationToken);

            await using var cmd = new NpgsqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("movieid", movieId);

            return await cmd.ExecuteScalarAsync(cancellationToken) as string;
        }

        /// <summary>
        /// Resolve a movie's mp4 key from the movies bucket by convention:
        /// files live at movies/{movieId}/{file}.mp4. The final source file to scan
        /// is tagged "SF" in its name (e.g. "..._SFv1.3.mp4") and there is normally
        /// exactly one; prefer it, falling back to the largest mp4. Null if none.
        /// </summary>
        private async Task<string?> ResolveR2KeyForMovieAsync(
            int movieId, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(_r2AccountId) ||
                string.IsNullOrWhiteSpace(_r2AccessKey) ||
                string.IsNullOrWhiteSpace(_r2SecretKey))
            {
                return null;
            }

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

            var response = await s3Client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = _r2BucketName,
                Prefix = $"{movieId}/"
            }, cancellationToken);

            var mp4s = (response.S3Objects ?? new List<S3Object>())
                .Where(o => o.Key.EndsWith(".mp4", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var sourceFiles = mp4s
                .Where(o => o.Key.Contains("SF", StringComparison.Ordinal))
                .ToList();

            return (sourceFiles.Count > 0 ? sourceFiles : mp4s)
                .OrderByDescending(o => o.Size)
                .Select(o => o.Key)
                .FirstOrDefault();
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
