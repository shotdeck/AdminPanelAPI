using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/movies/{movieId}/clips")]
    public sealed class ClipsController : ControllerBase
    {
        private const int DefaultPageSize = 20;
        private const int MaxPageSize = 100;
        private const int PresignedUrlExpiryMinutes = 60;

        private readonly IConfiguration _configuration;
        private readonly ILogger<ClipsController> _logger;

        public ClipsController(
            IConfiguration configuration,
            ILogger<ClipsController> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }

        [HttpGet]
        [ProducesResponseType(typeof(ClipPageResponse), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<ClipPageResponse>> GetClips(
            int movieId,
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = DefaultPageSize,
            CancellationToken ct = default)
        {
            if (movieId <= 0)
                return BadRequest("Invalid movieId.");

            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = DefaultPageSize;
            if (pageSize > MaxPageSize) pageSize = MaxPageSize;

            var accountId = _configuration["R2:AccountId"] ?? "";
            var accessKey = _configuration["R2:AccessKey"] ?? "";
            var secretKey = _configuration["R2:SecretKey"] ?? "";
            var bucketName = _configuration["R2:BucketName"] ?? "";

            if (string.IsNullOrWhiteSpace(accountId) ||
                string.IsNullOrWhiteSpace(accessKey) ||
                string.IsNullOrWhiteSpace(secretKey) ||
                string.IsNullOrWhiteSpace(bucketName))
            {
                _logger.LogError("R2 settings are missing.");
                return StatusCode(500, "R2 storage is not configured.");
            }

            var creds = new BasicAWSCredentials(accessKey.Trim(), secretKey.Trim());
            var config = new AmazonS3Config
            {
                ServiceURL = $"https://{accountId.Trim()}.r2.cloudflarestorage.com",
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false
            };

            using var client = new AmazonS3Client(creds, config);

            var prefix = $"clips_9s/{movieId}/";
            var allKeys = new List<string>();
            string? continuationToken = null;

            do
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = bucketName,
                    Prefix = prefix,
                    ContinuationToken = continuationToken
                };

                var response = await client.ListObjectsV2Async(request, ct);
                var objects = response.S3Objects ?? new List<S3Object>();

                foreach (var obj in objects)
                {
                    if (string.IsNullOrWhiteSpace(obj.Key))
                        continue;
                    if (!obj.Key.EndsWith(".mp4", StringComparison.OrdinalIgnoreCase))
                        continue;
                    allKeys.Add(obj.Key);
                }

                continuationToken = response.IsTruncated == true
                    ? response.NextContinuationToken
                    : null;

            } while (!string.IsNullOrEmpty(continuationToken));

            allKeys.Sort(StringComparer.OrdinalIgnoreCase);

            var totalCount = allKeys.Count;
            var totalPages = (int)Math.Ceiling(totalCount / (double)pageSize);
            var offset = (page - 1) * pageSize;
            var pageKeys = allKeys.Skip(offset).Take(pageSize).ToList();

            var clips = new List<ClipDto>();
            foreach (var key in pageKeys)
            {
                var url = client.GetPreSignedURL(new GetPreSignedUrlRequest
                {
                    BucketName = bucketName,
                    Key = key,
                    Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes),
                    Verb = HttpVerb.GET
                });

                clips.Add(new ClipDto
                {
                    FileName = Path.GetFileName(key),
                    Url = url
                });
            }

            return Ok(new ClipPageResponse
            {
                Clips = clips,
                Page = page,
                PageSize = pageSize,
                TotalCount = totalCount,
                TotalPages = totalPages,
                MovieId = movieId
            });
        }
    }

    public sealed class ClipDto
    {
        public string FileName { get; set; } = "";
        public string Url { get; set; } = "";
    }

    public sealed class ClipPageResponse
    {
        public List<ClipDto> Clips { get; set; } = new();
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int TotalCount { get; set; }
        public int TotalPages { get; set; }
        public int MovieId { get; set; }
    }
}
