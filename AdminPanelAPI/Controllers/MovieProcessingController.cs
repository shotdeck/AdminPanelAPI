using Microsoft.AspNetCore.Mvc;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MovieProcessingController : ControllerBase
    {
        private readonly IMovieProcessingJobRepository _jobRepository;
        private readonly IMovieJobQueue _jobQueue;

        public MovieProcessingController(
            IMovieProcessingJobRepository jobRepository,
            IMovieJobQueue jobQueue)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
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