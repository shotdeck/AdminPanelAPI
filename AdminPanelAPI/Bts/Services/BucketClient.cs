using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace AdminPanelAPI.Bts.Services;

/// <summary>
/// Thin wrapper over the BTS bucket. It works on full keys and knows nothing
/// about spaces; <see cref="SpaceFiles"/> builds every key it is handed.
/// Uploads and downloads never pass through the API: the browser is given a
/// short-lived presigned URL and talks to R2 directly.
/// </summary>
public sealed class BucketClient : IDisposable
{
    public static readonly TimeSpan UploadUrlLifetime = TimeSpan.FromMinutes(15);
    public static readonly TimeSpan ViewUrlLifetime = TimeSpan.FromMinutes(15);
    public static readonly TimeSpan DownloadUrlLifetime = TimeSpan.FromMinutes(5);
    private const int MaxListPage = 1000;

    private readonly AmazonS3Client _client;
    private readonly Protocol _protocol;

    public string Bucket { get; }

    /// <summary>Origin the browser talks to for presigned URLs (for CSP).</summary>
    public string Origin { get; }

    public BucketClient(IConfiguration configuration)
    {
        var accountId = (configuration["Bts:R2:AccountId"] ?? "").Trim();
        var accessKey = (configuration["Bts:R2:AccessKey"] ?? "").Trim();
        var secretKey = (configuration["Bts:R2:SecretKey"] ?? "").Trim();
        Bucket = (configuration["Bts:R2:BucketName"] ?? "bts").Trim();

        // Bts:R2:ServiceUrl points at another S3-compatible store (e.g. MinIO) for local testing.
        var serviceUrl = (configuration["Bts:R2:ServiceUrl"] ?? "").Trim();
        if (serviceUrl.Length == 0 && accountId.Length > 0)
            serviceUrl = $"https://{accountId}.r2.cloudflarestorage.com";

        if (serviceUrl.Length == 0 || accessKey.Length == 0 || secretKey.Length == 0)
            throw new InvalidOperationException("R2 credentials are not configured.");

        Origin = new Uri(serviceUrl).GetLeftPart(UriPartial.Authority);
        _protocol = Origin.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ? Protocol.HTTP : Protocol.HTTPS;

        _client = new AmazonS3Client(
            new BasicAWSCredentials(accessKey, secretKey),
            new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                ForcePathStyle = true,
                UseAccelerateEndpoint = false,
                UseDualstackEndpoint = false,
                EndpointDiscoveryEnabled = false,
                AuthenticationRegion = "auto",
                // R2 rejects the CRC checksums newer SDKs add by default.
                RequestChecksumCalculation = RequestChecksumCalculation.WHEN_REQUIRED,
                ResponseChecksumValidation = ResponseChecksumValidation.WHEN_REQUIRED
            });
    }

    public async Task<(List<string> Prefixes, List<S3Object> Objects, bool Truncated)> ListLevelAsync(
        string prefix, int maxEntries, CancellationToken ct)
    {
        var prefixes = new List<string>();
        var objects = new List<S3Object>();
        string? token = null;

        do
        {
            var page = await _client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = Bucket,
                Prefix = prefix,
                Delimiter = "/",
                MaxKeys = MaxListPage,
                ContinuationToken = token
            }, ct);

            prefixes.AddRange(page.CommonPrefixes ?? new List<string>());
            objects.AddRange(page.S3Objects ?? new List<S3Object>());
            token = page.IsTruncated == true ? page.NextContinuationToken : null;
        }
        while (token != null && prefixes.Count + objects.Count < maxEntries);

        return (prefixes, objects, token != null);
    }

    public async IAsyncEnumerable<S3Object> ListAllAsync(
        string prefix, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        string? token = null;
        do
        {
            var page = await _client.ListObjectsV2Async(new ListObjectsV2Request
            {
                BucketName = Bucket,
                Prefix = prefix,
                MaxKeys = MaxListPage,
                ContinuationToken = token
            }, ct);

            foreach (var obj in page.S3Objects ?? new List<S3Object>())
                yield return obj;

            token = page.IsTruncated == true ? page.NextContinuationToken : null;
        }
        while (token != null);
    }

    public async Task<bool> AnyUnderAsync(string prefix, CancellationToken ct)
    {
        var page = await _client.ListObjectsV2Async(new ListObjectsV2Request
        {
            BucketName = Bucket,
            Prefix = prefix,
            MaxKeys = 1
        }, ct);
        return (page.S3Objects?.Count ?? 0) > 0;
    }

    public async Task<long?> SizeAsync(string key, CancellationToken ct)
    {
        try
        {
            var head = await _client.GetObjectMetadataAsync(Bucket, key, ct);
            return head.ContentLength;
        }
        catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public Task PutEmptyAsync(string key, CancellationToken ct) =>
        _client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = Bucket,
            Key = key,
            ContentBody = "",
            DisablePayloadSigning = _protocol == Protocol.HTTPS
        }, ct);

    public string PresignPut(string key, string contentType) =>
        _client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            Protocol = _protocol,
            BucketName = Bucket,
            Key = key,
            Verb = HttpVerb.PUT,
            ContentType = contentType,
            Expires = DateTime.UtcNow.Add(UploadUrlLifetime)
        });

    public string PresignGet(string key, TimeSpan lifetime, string? attachmentName)
    {
        var request = new GetPreSignedUrlRequest
        {
            Protocol = _protocol,
            BucketName = Bucket,
            Key = key,
            Verb = HttpVerb.GET,
            Expires = DateTime.UtcNow.Add(lifetime)
        };
        if (attachmentName != null)
        {
            var ascii = new string(attachmentName.Select(c => c is >= ' ' and <= '~' and not '"' and not '\\' ? c : '_').ToArray());
            request.ResponseHeaderOverrides.ContentDisposition =
                $"attachment; filename=\"{ascii}\"; filename*=UTF-8''{Uri.EscapeDataString(attachmentName)}";
        }
        return _client.GetPreSignedURL(request);
    }

    public async Task<string> InitiateMultipartAsync(string key, string contentType, CancellationToken ct)
    {
        var response = await _client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = key,
            ContentType = contentType
        }, ct);
        return response.UploadId;
    }

    public string PresignPart(string key, string uploadId, int partNumber) =>
        _client.GetPreSignedURL(new GetPreSignedUrlRequest
        {
            Protocol = _protocol,
            BucketName = Bucket,
            Key = key,
            Verb = HttpVerb.PUT,
            UploadId = uploadId,
            PartNumber = partNumber,
            Expires = DateTime.UtcNow.Add(UploadUrlLifetime)
        });

    public Task CompleteMultipartAsync(string key, string uploadId, IEnumerable<(int Number, string ETag)> parts, CancellationToken ct) =>
        _client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = key,
            UploadId = uploadId,
            PartETags = parts.OrderBy(p => p.Number).Select(p => new PartETag(p.Number, p.ETag)).ToList()
        }, ct);

    public Task AbortMultipartAsync(string key, string uploadId, CancellationToken ct) =>
        _client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
        {
            BucketName = Bucket,
            Key = key,
            UploadId = uploadId
        }, ct);

    public async Task MoveAsync(string sourceKey, string targetKey, CancellationToken ct)
    {
        await _client.CopyObjectAsync(new CopyObjectRequest
        {
            SourceBucket = Bucket,
            SourceKey = sourceKey,
            DestinationBucket = Bucket,
            DestinationKey = targetKey
        }, ct);
        await _client.DeleteObjectAsync(Bucket, sourceKey, ct);
    }

    /// <summary>Moves everything under one prefix to another; returns how many objects moved.</summary>
    public async Task<int> MovePrefixAsync(string sourcePrefix, string targetPrefix, CancellationToken ct)
    {
        // Collect first: moving while paging the same prefix would skip keys.
        var keys = new List<string>();
        await foreach (var obj in ListAllAsync(sourcePrefix, ct))
            keys.Add(obj.Key);

        foreach (var key in keys)
        {
            var target = targetPrefix + key[sourcePrefix.Length..];
            if (key.EndsWith('/'))
            {
                await PutEmptyAsync(target, ct);
                await _client.DeleteObjectAsync(Bucket, key, ct);
            }
            else
            {
                await MoveAsync(key, target, ct);
            }
        }
        return keys.Count;
    }

    public Task DeleteAsync(string key, CancellationToken ct) => _client.DeleteObjectAsync(Bucket, key, ct);

    public void Dispose() => _client.Dispose();
}
