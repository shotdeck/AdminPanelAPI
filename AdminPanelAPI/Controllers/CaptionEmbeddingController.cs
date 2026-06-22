using Microsoft.AspNetCore.Mvc;

namespace AdminPanelAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CaptionEmbeddingController : ControllerBase
    {
        private readonly ICaptionEmbeddingJobRepository _jobRepository;
        private readonly ICaptionEmbeddingJobQueue _jobQueue;
        private readonly ICaptionEmbeddingService _service;

        public CaptionEmbeddingController(
            ICaptionEmbeddingJobRepository jobRepository,
            ICaptionEmbeddingJobQueue jobQueue,
            ICaptionEmbeddingService service)
        {
            _jobRepository = jobRepository;
            _jobQueue = jobQueue;
            _service = service;
        }

        [HttpPost("process/{imageId:int}")]
        public async Task<IActionResult> ProcessSingleImage(
            int imageId,
            CancellationToken cancellationToken = default)
        {
            var result = await _service.ProcessSingleImageAsync(imageId, cancellationToken);

            if (result.Status == "Failed" && result.Error?.Contains("not found") == true)
                return NotFound(result);

            return Ok(result);
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

        [HttpPost("process-all")]
        public async Task<IActionResult> ProcessAll(
            [FromQuery] int concurrency = 20,
            CancellationToken cancellationToken = default)
        {
            if (concurrency <= 0 || concurrency > 100)
                return BadRequest(new { error = "concurrency must be between 1 and 100" });

            var jobId = await _jobRepository.CreateJobAsync(-concurrency, cancellationToken);

            await _jobQueue.QueueJobAsync(jobId, cancellationToken);

            return Ok(new
            {
                jobId,
                concurrency,
                mode = "process-all",
                status = "Queued",
                checkProgressUrl = $"/api/CaptionEmbedding/status/{jobId}"
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
