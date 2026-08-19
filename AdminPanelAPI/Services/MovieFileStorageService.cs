using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using AdminPanelAPI.Models;
using System.Text.RegularExpressions;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Browsing and transfer operations against the R2 bucket that holds movie
    /// source files (the same bucket dialogue transcription uploads to, keyed
    /// by movie id: "{movieId}/{fileName}").
    ///
    /// Uploads are never proxied through this API: the browser gets a presigned
    /// URL and talks to R2 directly, which keeps multi-GB movie files off the
    /// App Service request path.
    /// </summary>
    public interface IMovieFileStorageService
    {
        Task<MovieFileListResponse> ListAsync(
            string prefix, bool mp4Only, string? continuationToken, int pageSize, CancellationToken ct);

        Task CreateFolderAsync(string prefix, CancellationToken ct);

        Task<PresignedUploadResponse> CreateSingleUploadAsync(
            string prefix, string fileName, MovieFileVariant variant, string? contentType, CancellationToken ct);

        Task<MultipartUploadResponse> InitiateMultipartUploadAsync(
            string prefix, string fileName, MovieFileVariant variant, string? contentType, CancellationToken ct);

        IReadOnlyList<PresignedPart> CreatePartUrls(string key, string uploadId, IEnumerable<int> partNumbers);

        Task CompleteMultipartUploadAsync(
            string key, string uploadId, IEnumerable<MultipartPart> parts, CancellationToken ct);

        Task AbortMultipartUploadAsync(string key, string uploadId, CancellationToken ct);

        string CreateDownloadUrl(string key, bool asAttachment);

        Task RenameObjectAsync(string key, string newName, CancellationToken ct);

        Task DeleteObjectAsync(string key, CancellationToken ct);

        Task<MovePrefixResponse> MovePrefixAsync(
            string sourcePrefix, string targetPrefix, bool deleteSource, CancellationToken ct);

        Task<int> DeletePrefixAsync(string prefix, CancellationToken ct);
    }

    public sealed class MovieFileStorageService : IMovieFileStorageService, IDisposable
    {
        /// <summary>Uploads below this size go through a single presigned PUT.</summary>
        public const long MultipartThresholdBytes = 100L * 1024 * 1024;

        public const int MultipartPartSizeBytes = 16 * 1024 * 1024;

        /// <summary>
        /// Files uploaded before their frl_movies record exists live here, in a
        /// batch folder, and are moved to "{movieId}/" once the id is known.
        /// It is also the only prefix where recursive delete is permitted.
        /// </summary>
        public const string StagingRoot = "_staging/";

        private const int PresignedUrlExpiryMinutes = 60;
        private const int MaxListPageSize = 1000;

        private static readonly Regex HdPattern =
            new(@"(^|[^a-z0-9])(hd|1080p?|2160p?|4k|uhd)([^a-z0-9]|$)",
                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static readonly Regex SlimPattern =
            new(@"(^|[^a-z0-9])(proxy|slim|sd|low|540p?|720p?)([^a-z0-9]|$)",
                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private readonly AmazonS3Client _client;
        private readonly string _bucketName;
        private readonly ILogger<MovieFileStorageService> _logger;

        public MovieFileStorageService(IConfiguration configuration, ILogger<MovieFileStorageService> logger)
        {
            _logger = logger;

            var accountId = (configuration["R2:AccountId"] ?? "").Trim();
            var accessKey = (configuration["R2:AccessKey"] ?? "").Trim();
            var secretKey = (configuration["R2:SecretKey"] ?? "").Trim();

            _bucketName = (configuration["MovieFiles:BucketName"]
                ?? configuration["DialogueSearch:R2BucketName"]
                ?? "movies").Trim();

            if (accountId.Length == 0 || accessKey.Length == 0 || secretKey.Length == 0)
                throw new InvalidOperationException("R2 credentials are not configured.");

            _client = new AmazonS3Client(
                new BasicAWSCredentials(accessKey, secretKey),
                new AmazonS3Config
                {
                    ServiceURL = $"https://{accountId}.r2.cloudflarestorage.com",
                    ForcePathStyle = true,
                    UseAccelerateEndpoint = false,
                    UseDualstackEndpoint = false,
                    EndpointDiscoveryEnabled = false
                });
        }

        public async Task<MovieFileListResponse> ListAsync(
            string prefix, bool mp4Only, string? continuationToken, int pageSize, CancellationToken ct)
        {
            prefix = NormalizePrefix(prefix);
            pageSize = Math.Clamp(pageSize, 1, MaxListPageSize);

            var request = new ListObjectsV2Request
            {
                BucketName = _bucketName,
                Prefix = prefix,
                Delimiter = "/",
                MaxKeys = pageSize,
                ContinuationToken = string.IsNullOrWhiteSpace(continuationToken) ? null : continuationToken
            };

            var response = await _client.ListObjectsV2Async(request, ct);

            var folders = (response.CommonPrefixes ?? new List<string>())
                .Select(p => new MovieFolderDto
                {
                    Prefix = p,
                    Name = p[prefix.Length..].TrimEnd('/')
                })
                .OrderBy(f => f.Name, NaturalOrder.Comparer)
                .ToList();

            var files = new List<MovieFileDto>();
            foreach (var obj in response.S3Objects ?? new List<S3Object>())
            {
                // The zero-byte object that keeps an otherwise empty folder visible.
                if (obj.Key.EndsWith("/", StringComparison.Ordinal))
                    continue;

                var name = obj.Key[prefix.Length..];
                if (mp4Only && !name.EndsWith(".mp4", StringComparison.OrdinalIgnoreCase))
                    continue;

                files.Add(new MovieFileDto
                {
                    Key = obj.Key,
                    Name = name,
                    SizeBytes = obj.Size ?? 0,
                    LastModified = obj.LastModified,
                    Variant = DetectVariant(name).ToString().ToLowerInvariant()
                });
            }

            return new MovieFileListResponse
            {
                Bucket = _bucketName,
                Prefix = prefix,
                ParentPrefix = ParentPrefix(prefix),
                Folders = folders,
                Files = files,
                NextToken = response.IsTruncated == true ? response.NextContinuationToken : null,
                StagingRoot = StagingRoot
            };
        }

        public async Task CreateFolderAsync(string prefix, CancellationToken ct)
        {
            prefix = NormalizePrefix(prefix);
            if (prefix.Length == 0)
                throw new ArgumentException("A folder name is required.");

            await _client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = _bucketName,
                Key = prefix,
                ContentBody = "",
                DisablePayloadSigning = true
            }, ct);
        }

        public async Task<PresignedUploadResponse> CreateSingleUploadAsync(
            string prefix, string fileName, MovieFileVariant variant, string? contentType, CancellationToken ct)
        {
            var key = BuildKey(prefix, fileName, variant);
            var resolvedType = ResolveContentType(contentType, key);

            var exists = await ObjectExistsAsync(key, ct);

            var url = _client.GetPreSignedURL(new GetPreSignedUrlRequest
            {
                BucketName = _bucketName,
                Key = key,
                Verb = HttpVerb.PUT,
                ContentType = resolvedType,
                Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes)
            });

            return new PresignedUploadResponse
            {
                Key = key,
                Url = url,
                ContentType = resolvedType,
                Overwrites = exists
            };
        }

        public async Task<MultipartUploadResponse> InitiateMultipartUploadAsync(
            string prefix, string fileName, MovieFileVariant variant, string? contentType, CancellationToken ct)
        {
            var key = BuildKey(prefix, fileName, variant);
            var resolvedType = ResolveContentType(contentType, key);

            var exists = await ObjectExistsAsync(key, ct);

            var initiated = await _client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                ContentType = resolvedType
            }, ct);

            return new MultipartUploadResponse
            {
                Key = key,
                UploadId = initiated.UploadId,
                PartSizeBytes = MultipartPartSizeBytes,
                ContentType = resolvedType,
                Overwrites = exists
            };
        }

        public IReadOnlyList<PresignedPart> CreatePartUrls(string key, string uploadId, IEnumerable<int> partNumbers)
        {
            key = ValidateKey(key);

            return partNumbers
                .Select(partNumber => new PresignedPart
                {
                    PartNumber = partNumber,
                    Url = _client.GetPreSignedURL(new GetPreSignedUrlRequest
                    {
                        BucketName = _bucketName,
                        Key = key,
                        Verb = HttpVerb.PUT,
                        UploadId = uploadId,
                        PartNumber = partNumber,
                        Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes)
                    })
                })
                .ToList();
        }

        public async Task CompleteMultipartUploadAsync(
            string key, string uploadId, IEnumerable<MultipartPart> parts, CancellationToken ct)
        {
            key = ValidateKey(key);

            await _client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                UploadId = uploadId,
                PartETags = parts
                    .OrderBy(p => p.PartNumber)
                    .Select(p => new PartETag(p.PartNumber, p.ETag))
                    .ToList()
            }, ct);
        }

        public async Task AbortMultipartUploadAsync(string key, string uploadId, CancellationToken ct)
        {
            key = ValidateKey(key);

            await _client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
            {
                BucketName = _bucketName,
                Key = key,
                UploadId = uploadId
            }, ct);
        }

        public string CreateDownloadUrl(string key, bool asAttachment)
        {
            key = ValidateKey(key);

            var request = new GetPreSignedUrlRequest
            {
                BucketName = _bucketName,
                Key = key,
                Verb = HttpVerb.GET,
                Expires = DateTime.UtcNow.AddMinutes(PresignedUrlExpiryMinutes)
            };

            if (asAttachment)
            {
                var fileName = key[(key.LastIndexOf('/') + 1)..];
                request.ResponseHeaderOverrides.ContentDisposition =
                    $"attachment; filename=\"{fileName.Replace("\"", "")}\"";
            }

            return _client.GetPreSignedURL(request);
        }

        public async Task RenameObjectAsync(string key, string newName, CancellationToken ct)
        {
            key = ValidateKey(key);

            var sanitized = SanitizeFileName(newName);
            var slash = key.LastIndexOf('/');
            var targetKey = slash < 0 ? sanitized : key[..(slash + 1)] + sanitized;

            if (targetKey == key)
                return;

            await _client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = _bucketName,
                SourceKey = key,
                DestinationBucket = _bucketName,
                DestinationKey = targetKey
            }, ct);

            await _client.DeleteObjectAsync(_bucketName, key, ct);
        }

        public async Task DeleteObjectAsync(string key, CancellationToken ct)
        {
            key = ValidateKey(key);
            await _client.DeleteObjectAsync(_bucketName, key, ct);
        }

        public async Task<MovePrefixResponse> MovePrefixAsync(
            string sourcePrefix, string targetPrefix, bool deleteSource, CancellationToken ct)
        {
            sourcePrefix = NormalizePrefix(sourcePrefix);
            targetPrefix = NormalizePrefix(targetPrefix);

            if (sourcePrefix.Length == 0 || targetPrefix.Length == 0)
                throw new ArgumentException("Both a source and a target folder are required.");

            if (targetPrefix.StartsWith(sourcePrefix, StringComparison.Ordinal))
                throw new ArgumentException("The target folder cannot sit inside the source folder.");

            var moved = 0;
            long bytes = 0;
            string? token = null;

            do
            {
                var page = await _client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = _bucketName,
                    Prefix = sourcePrefix,
                    ContinuationToken = token,
                    MaxKeys = MaxListPageSize
                }, ct);

                foreach (var obj in page.S3Objects ?? new List<S3Object>())
                {
                    var relative = obj.Key[sourcePrefix.Length..];
                    var destinationKey = targetPrefix + relative;

                    if (obj.Key.EndsWith("/", StringComparison.Ordinal))
                    {
                        // Folder marker: recreate rather than copy, then drop it.
                        if (relative.Length > 0)
                            await CreateFolderAsync(destinationKey, ct);
                    }
                    else
                    {
                        await _client.CopyObjectAsync(new CopyObjectRequest
                        {
                            SourceBucket = _bucketName,
                            SourceKey = obj.Key,
                            DestinationBucket = _bucketName,
                            DestinationKey = destinationKey
                        }, ct);

                        moved++;
                        bytes += obj.Size ?? 0;
                    }

                    if (deleteSource)
                        await _client.DeleteObjectAsync(_bucketName, obj.Key, ct);
                }

                token = page.IsTruncated == true ? page.NextContinuationToken : null;
            }
            while (token != null);

            _logger.LogInformation(
                "Moved {Count} movie file(s) from {Source} to {Target} (deleteSource={DeleteSource}).",
                moved, sourcePrefix, targetPrefix, deleteSource);

            return new MovePrefixResponse
            {
                SourcePrefix = sourcePrefix,
                TargetPrefix = targetPrefix,
                FilesMoved = moved,
                BytesMoved = bytes,
                SourceDeleted = deleteSource
            };
        }

        public async Task<int> DeletePrefixAsync(string prefix, CancellationToken ct)
        {
            prefix = NormalizePrefix(prefix);

            // Recursive delete is destructive and this bucket is production, so
            // it is confined to the staging area; everything else goes one
            // object at a time through the UI.
            if (!prefix.StartsWith(StagingRoot, StringComparison.Ordinal) || prefix == StagingRoot)
                throw new ArgumentException($"Recursive delete is only allowed inside '{StagingRoot}'.");

            var deleted = 0;
            string? token = null;

            do
            {
                var page = await _client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = _bucketName,
                    Prefix = prefix,
                    ContinuationToken = token,
                    MaxKeys = MaxListPageSize
                }, ct);

                foreach (var obj in page.S3Objects ?? new List<S3Object>())
                {
                    await _client.DeleteObjectAsync(_bucketName, obj.Key, ct);
                    deleted++;
                }

                token = page.IsTruncated == true ? page.NextContinuationToken : null;
            }
            while (token != null);

            return deleted;
        }

        // ── Keys, names and variants ───────────────────────────────────

        public static MovieFileVariant DetectVariant(string fileName)
        {
            var stem = fileName;
            var dot = stem.LastIndexOf('.');
            if (dot > 0)
                stem = stem[..dot];

            if (SlimPattern.IsMatch(stem))
                return MovieFileVariant.Slim;

            if (HdPattern.IsMatch(stem))
                return MovieFileVariant.Hd;

            return MovieFileVariant.Unknown;
        }

        /// <summary>
        /// Tags the file name with its variant so HD and slimmed copies of the
        /// same movie can share a folder without colliding. A name that already
        /// says what it is (e.g. "...BR.HD.01_SF.mp4") is left alone.
        /// </summary>
        public static string ApplyVariantSuffix(string fileName, MovieFileVariant variant)
        {
            if (variant == MovieFileVariant.Unknown || DetectVariant(fileName) == variant)
                return fileName;

            var stem = fileName;
            var extension = "";
            var dot = fileName.LastIndexOf('.');
            if (dot > 0)
            {
                stem = fileName[..dot];
                extension = fileName[dot..];
            }

            var suffix = variant == MovieFileVariant.Hd ? "HD" : "PROXY";
            return $"{stem}_{suffix}{extension}";
        }

        public static string SanitizeFileName(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                throw new ArgumentException("A file name is required.");

            // Guard against a name that walks out of its folder.
            fileName = fileName.Replace('\\', '/');
            fileName = fileName[(fileName.LastIndexOf('/') + 1)..].Trim();

            if (fileName.Length == 0 || fileName == "." || fileName == "..")
                throw new ArgumentException("A valid file name is required.");

            var cleaned = new string(fileName
                .Select(c => char.IsControl(c) || "?#%\"<>|*:".Contains(c) ? '_' : c)
                .ToArray());

            return cleaned;
        }

        public static string NormalizePrefix(string? prefix)
        {
            if (string.IsNullOrWhiteSpace(prefix))
                return "";

            var normalized = prefix.Replace('\\', '/').Trim().TrimStart('/');

            if (normalized.Split('/').Any(segment => segment == ".." || segment == "."))
                throw new ArgumentException("Invalid folder path.");

            if (normalized.Length > 0 && !normalized.EndsWith("/", StringComparison.Ordinal))
                normalized += "/";

            return normalized;
        }

        public static string ValidateKey(string? key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("A key is required.");

            var normalized = key.Replace('\\', '/').Trim().TrimStart('/');

            if (normalized.EndsWith("/", StringComparison.Ordinal))
                throw new ArgumentException("A file key is required, not a folder.");

            if (normalized.Split('/').Any(segment => segment == ".." || segment == "."))
                throw new ArgumentException("Invalid key.");

            return normalized;
        }

        private static string BuildKey(string prefix, string fileName, MovieFileVariant variant)
        {
            var normalizedPrefix = NormalizePrefix(prefix);
            var name = ApplyVariantSuffix(SanitizeFileName(fileName), variant);
            return normalizedPrefix + name;
        }

        private static string ParentPrefix(string prefix)
        {
            if (prefix.Length == 0)
                return "";

            var trimmed = prefix.TrimEnd('/');
            var slash = trimmed.LastIndexOf('/');
            return slash < 0 ? "" : trimmed[..(slash + 1)];
        }

        private static string ResolveContentType(string? contentType, string key)
        {
            if (!string.IsNullOrWhiteSpace(contentType))
                return contentType.Trim();

            return key.EndsWith(".mp4", StringComparison.OrdinalIgnoreCase)
                ? "video/mp4"
                : "application/octet-stream";
        }

        private async Task<bool> ObjectExistsAsync(string key, CancellationToken ct)
        {
            try
            {
                await _client.GetObjectMetadataAsync(_bucketName, key, ct);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        public void Dispose() => _client.Dispose();
    }

    /// <summary>
    /// Orders folder names so movie ids read 2, 10, 100 rather than 10, 100, 2.
    /// </summary>
    internal static class NaturalOrder
    {
        public static readonly IComparer<string> Comparer = new NaturalComparer();

        private sealed class NaturalComparer : IComparer<string>
        {
            public int Compare(string? a, string? b)
            {
                var aIsNumber = long.TryParse(a, out var aNumber);
                var bIsNumber = long.TryParse(b, out var bNumber);

                if (aIsNumber && bIsNumber)
                    return aNumber.CompareTo(bNumber);

                if (aIsNumber != bIsNumber)
                    return aIsNumber ? -1 : 1;

                return string.Compare(a, b, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
