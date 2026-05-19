using Microsoft.AspNetCore.Mvc;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CaptionEmbeddingController : ControllerBase
    {
        private readonly ICaptionEmbeddingJobRepository _jobRepository;
        private readonly ICaptionEmbeddingJobQueue _jobQueue;

        public CaptionEmbeddingController(
            ICaptionEmbeddingJobRepository jobRepository,
            ICaptionEmbeddingJobQueue jobQueue)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
        }

        [HttpPost("start-batch")]
        public async Task<IActionResult> StartBatch(
            [FromQuery] int batchSize = 50,
            CancellationToken cancellationToken = default)
        {
            if (batchSize <= 0)
                return BadRequest(new { error = "batchSize must be greater than 0" });

            var jobId = await _jobRepository.CreateJobAsync(batchSize, cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                batchSize,
                status = "Queued"
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
