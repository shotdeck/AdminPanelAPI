using AdminPanelAPI.Interfaces;
using AdminPanelAPI.Services;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Data.Common;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MovieProcessingController : ControllerBase
    {
        private readonly IMovieProcessingJobRepository _jobRepository;
        private readonly IMovieJobQueue _jobQueue;
        private readonly IMovieProcessingService _processingService;
        private readonly IClipPreviewService _clipPreviewService;
        private readonly IClipPreviewMotionRunner _clipPreviewRunner;
        private readonly IConfiguration _configuration;
        private readonly NpgsqlConnection _connection;
        private readonly MovieWorkerDiagnostics _workerDiagnostics;

        public MovieProcessingController(
            IMovieProcessingJobRepository jobRepository,
            IMovieJobQueue jobQueue,
            IMovieProcessingService processingService,
            IClipPreviewService clipPreviewService,
            IClipPreviewMotionRunner clipPreviewRunner,
            NpgsqlConnection connection,
            IConfiguration configuration,
            MovieWorkerDiagnostics workerDiagnostics)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
            _processingService = processingService;
            _clipPreviewService = clipPreviewService;
            _clipPreviewRunner = clipPreviewRunner;
            _configuration = configuration;
            _connection = connection;
            _workerDiagnostics = workerDiagnostics;
        }

        /// <summary>
        /// Reports whether the movie worker loops are alive and what their last
        /// claim attempt did.
        /// </summary>
        [HttpGet("worker-health")]
        public IActionResult GetWorkerHealth()
        {
            return Ok(_workerDiagnostics.Snapshot());
        }
        [HttpGet("db-info")]
        public async Task<IActionResult> GetDatabaseInfo(
     CancellationToken cancellationToken)
        {
           
           
            const string sql = @"
SELECT
    current_database() AS database,
    inet_server_addr() AS server_ip,
    inet_server_port() AS server_port,
    current_user AS db_user,
    version() AS postgres_version;
";

            await using var cmd = new NpgsqlCommand(sql, _connection);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

            if (await reader.ReadAsync(cancellationToken))
            {
                return Ok(new
                {
                    database = reader["database"]?.ToString(),
                    serverIp = reader["server_ip"]?.ToString(),
                    serverPort = reader["server_port"]?.ToString(),
                    user = reader["db_user"]?.ToString(),
                    version = reader["postgres_version"]?.ToString(),
                    connection = MaskConnectionString(_configuration["ConnectionStrings: Default"]),

                    sshTunnel = new
                    {
                        enabled = _configuration.GetValue<bool>("SshTunnel:Enabled"),
                        sshHost = _configuration["SshTunnel:SshHost"],
                        sshPort = _configuration["SshTunnel:SshPort"],
                        sshUser = _configuration["SshTunnel:SshUser"],

                        remoteDbHost = _configuration["SshTunnel:RemoteDbHost"],
                        remoteDbPort = _configuration["SshTunnel:RemoteDbPort"],

                        localBindHost = _configuration["SshTunnel:LocalBindHost"],
                        localBindPort = _configuration["SshTunnel:LocalBindPort"],

                        reconnectDelayMs = _configuration["SshTunnel:ReconnectDelayMs"],

                        // Do not return the actual key path if you consider it sensitive.
                        sshKeyPathConfigured = !string.IsNullOrWhiteSpace(
                            _configuration["SshTunnel:SshKeyPath"])
                    }
                });
            }

            return StatusCode(500, "Failed to read database info.");
        }

        private string MaskConnectionString(string conn)
        {
            if (string.IsNullOrWhiteSpace(conn))
                return "";

            return System.Text.RegularExpressions.Regex.Replace(
                conn,
                @"(Password|Pwd)=([^;]+)",
                "$1=****",
                System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        }


        [HttpPost("process/{movieId:int}")]
        public async Task<IActionResult> ProcessMovie(
            int movieId,
            [FromQuery] double threshold = 0.7,
            [FromQuery] bool overwrite = false,
            [FromQuery] bool missingOnly = false,
            CancellationToken cancellationToken = default)
        {
            if (overwrite && missingOnly)
                return BadRequest(new { error = "overwrite and missingOnly cannot both be set." });

            var jobId = await _jobRepository.CreateJobAsync(
                movieId,
                threshold,
                overwrite,
                missingOnly,
                cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                movieId,
                threshold,
                overwrite,
                missingOnly,
                status = "Queued"
            });
        }

        /// <summary>
        /// Reports which clips of an already processed movie still have no scene
        /// boundary, and which live images have no clip in R2 at all.
        /// </summary>
        [HttpGet("missing-clips/{movieId:int}")]
        public async Task<IActionResult> GetMissingClips(
            int movieId,
            CancellationToken cancellationToken = default)
        {
            var report = await _processingService.GetMissingClipReportAsync(movieId, cancellationToken);

            return Ok(report);
        }

        /// <summary>
        /// Reports every movie that still has live images without a scene boundary,
        /// worst first, along with the totals across all movies.
        /// </summary>
        [HttpGet("missing-clips-summary")]
        public async Task<IActionResult> GetMissingClipsSummary(
            [FromQuery] int limit = 100,
            CancellationToken cancellationToken = default)
        {
            var summary = await _jobRepository.GetMissingClipSummaryAsync(limit, cancellationToken);

            return Ok(summary);
        }

        /// <summary>
        /// Cuts a 480p silent preview of each detected scene for one movie into
        /// R2 at clips_9s/{movieId}/{randid}_short.mp4.
        /// </summary>
        [HttpPost("clip-previews/{movieId:int}")]
        public async Task<IActionResult> GenerateClipPreviews(
            int movieId,
            [FromQuery] bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            var result = await _clipPreviewService.GeneratePreviewsForMovieAsync(
                movieId,
                overwrite,
                cancellationToken);

            return Ok(result);
        }

        /// <summary>
        /// Walks movies with scene boundaries in movieid order and cuts any preview
        /// that is not in R2 yet. Call repeatedly, passing back nextAfterMovieId.
        /// </summary>
        [HttpPost("clip-previews-backfill")]
        public async Task<IActionResult> BackfillClipPreviews(
            [FromQuery] int movieCount = 20,
            [FromQuery] int afterMovieId = 0,
            [FromQuery] bool overwrite = false,
            CancellationToken cancellationToken = default)
        {
            if (movieCount <= 0)
                return BadRequest(new { error = "movieCount must be greater than 0" });

            var result = await _clipPreviewService.BackfillAsync(
                movieCount,
                afterMovieId,
                overwrite,
                cancellationToken);

            return Ok(result);
        }

        /// <summary>
        /// One batch of previews, oldest image first. Call repeatedly with nextAfterImageId.
        /// motionOnly=true limits it to the clips a Motion filter can select (images with a
        /// camera-movement tag); motionOnly=false covers every clip with a scene boundary.
        /// </summary>
        [HttpPost("clip-previews-motion")]
        public async Task<IActionResult> GenerateMotionClipPreviews(
            [FromQuery] int limit = 2000,
            [FromQuery] int afterImageId = 0,
            [FromQuery] bool overwrite = false,
            [FromQuery] bool motionOnly = true,
            CancellationToken cancellationToken = default)
        {
            if (limit <= 0)
                return BadRequest(new { error = "limit must be greater than 0" });

            var result = await _clipPreviewService.GeneratePreviewBatchAsync(
                limit,
                afterImageId,
                overwrite,
                motionOnly,
                cancellationToken);

            return Ok(result);
        }

        /// <summary>
        /// Previews every eligible clip in one go. Returns as soon as the run starts and
        /// keeps going in the background, so nothing has to be re-triggered. Watch it with
        /// GET clip-previews-motion-all. motionOnly=false runs the whole library instead of
        /// just the motion-tagged clips, which is a much longer and more expensive run.
        /// </summary>
        [HttpPost("clip-previews-motion-all")]
        public IActionResult StartMotionClipPreviewRun(
            [FromQuery] int afterImageId = 0,
            [FromQuery] int batchSize = 2000,
            [FromQuery] bool overwrite = false,
            [FromQuery] bool motionOnly = true)
        {
            if (batchSize <= 0 || batchSize > 20000)
                return BadRequest(new { error = "batchSize must be between 1 and 20000" });

            var (started, status) = _clipPreviewRunner.Start(afterImageId, batchSize, overwrite, motionOnly);

            if (!started)
            {
                return Conflict(new
                {
                    error = "A clip preview run is already going. Stop it first, or wait for it to finish.",
                    status
                });
            }

            return Accepted(status);
        }

        /// <summary>
        /// State of the background preview run, including the batch it is on.
        /// </summary>
        [HttpGet("clip-previews-motion-all")]
        public IActionResult GetMotionClipPreviewRun()
        {
            return Ok(_clipPreviewRunner.GetStatus());
        }

        /// <summary>
        /// Stops the background run after the batch it is on. Restarting it later from
        /// the reported cursor (or from 0) picks up where it left off.
        /// </summary>
        [HttpPost("clip-previews-motion-all/stop")]
        public IActionResult StopMotionClipPreviewRun()
        {
            return Ok(_clipPreviewRunner.Stop());
        }

        /// <summary>
        /// Coverage of the clip-preview run: clips eligible for a preview, how many the
        /// given cursor has passed, and how many previews are in R2. Pass countR2=false
        /// for a database-only answer that returns instantly, and motionOnly=false to
        /// count against the whole library rather than the motion-tagged clips.
        /// </summary>
        [HttpGet("clip-previews-progress")]
        public async Task<IActionResult> GetClipPreviewProgress(
            [FromQuery] int afterImageId = 0,
            [FromQuery] int? movieId = null,
            [FromQuery] bool countR2 = true,
            [FromQuery] long maxObjectsToCount = 2_000_000,
            [FromQuery] bool motionOnly = true,
            CancellationToken cancellationToken = default)
        {
            if (maxObjectsToCount <= 0)
                return BadRequest(new { error = "maxObjectsToCount must be greater than 0" });

            var progress = await _clipPreviewService.GetPreviewProgressAsync(
                afterImageId,
                movieId,
                countR2,
                maxObjectsToCount,
                motionOnly,
                cancellationToken);

            return Ok(progress);
        }

        /// <summary>
        /// Queues movies that completed the pipeline but still have images without
        /// a scene boundary, so only the missed clips get scene detection.
        /// </summary>
        [HttpPost("reprocess-missing-batch")]
        public async Task<IActionResult> ReprocessMissingBatch(
            [FromQuery] int count = 50,
            [FromQuery] double threshold = 0.7,
            CancellationToken cancellationToken = default)
        {
            if (count <= 0)
                return BadRequest(new { error = "count must be greater than 0" });

            var movieIds = await _jobRepository.GetMovieIdsWithMissingClipsAsync(
                count,
                cancellationToken);

            var jobs = new List<object>();

            foreach (var movieId in movieIds)
            {
                var jobId = await _jobRepository.CreateJobAsync(
                    movieId,
                    threshold,
                    false,
                    true,
                    cancellationToken);

                await _jobQueue.QueueJobAsync(jobId, cancellationToken);

                jobs.Add(new
                {
                    jobId,
                    movieId,
                    status = "Queued"
                });
            }

            return Ok(new
            {
                requested = count,
                queued = jobs.Count,
                threshold,
                missingOnly = true,
                jobs
            });
        }

        [HttpPost("start-batch")]
        public async Task<IActionResult> StartBatch(
    [FromQuery] int count = 50,
    [FromQuery] double threshold = 0.7,
    [FromQuery] bool overwrite = false,
    CancellationToken cancellationToken = default)
        {
            if (count <= 0)
                return BadRequest(new { error = "count must be greater than 0" });

            

            var movieIds = await _jobRepository.GetUnprocessedMovieIdsAsync(
                count,
                cancellationToken);

            var jobs = new List<object>();

            foreach (var movieId in movieIds)
            {
                var jobId = await _jobRepository.CreateJobAsync(
                    movieId,
                    threshold,
                    overwrite,
                    false,
                    cancellationToken);

                await _jobQueue.QueueJobAsync(jobId, cancellationToken);

                jobs.Add(new
                {
                    jobId,
                    movieId,
                    status = "Queued"
                });
            }

            return Ok(new
            {
                requested = count,
                queued = jobs.Count,
                threshold,
                overwrite,
                jobs
            });
        }

        [HttpGet("status/{jobId:long}")]
        public async Task<IActionResult> GetStatus(
            long jobId,
            CancellationToken cancellationToken = default)
        {
            var job = await _jobRepository.GetJobAsync(jobId, cancellationToken);

            if (job == null)
            {
                return NotFound(new
                {
                    message = $"Job {jobId} not found."
                });
            }

            return Ok(job);
        }

        /// <summary>
        /// Puts a job back in the queue, for one left Running by a crashed or
        /// restarted worker.
        /// </summary>
        [HttpPost("requeue/{jobId:long}")]
        public async Task<IActionResult> RequeueJob(
            long jobId,
            CancellationToken cancellationToken = default)
        {
            var requeued = await _jobRepository.RequeueJobAsync(jobId, cancellationToken);

            if (!requeued)
            {
                return NotFound(new
                {
                    message = $"Job {jobId} not found."
                });
            }

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                status = "Queued"
            });
        }
    }
}