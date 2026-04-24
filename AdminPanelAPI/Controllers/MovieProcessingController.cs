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
        private readonly IConfiguration _configuration;
        private readonly NpgsqlConnection _connection;

        public MovieProcessingController(
            IMovieProcessingJobRepository jobRepository,
            IMovieJobQueue jobQueue,
            NpgsqlConnection connection,
            IConfiguration configuration)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
            _configuration = configuration;
            _connection = connection;
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
            CancellationToken cancellationToken = default)
        {
            var jobId = await _jobRepository.CreateJobAsync(
                movieId,
                threshold,
                overwrite,
                cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                movieId,
                threshold,
                overwrite,
                status = "Queued"
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
    }
}