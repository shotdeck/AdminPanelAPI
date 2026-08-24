namespace AdminPanelAPI.Models
{
    /// <summary>
    /// Which copy of a movie a file is: the full-quality master or the slimmed
    /// down version used for analysis and playback.
    /// </summary>
    public enum MovieFileVariant
    {
        Unknown = 0,
        Hd = 1,
        Slim = 2
    }

    public static class MovieFileVariantParser
    {
        /// <summary>
        /// Accepts what the dashboard sends ("hd", "slim", "proxy", "") rather
        /// than relying on enum binding, which rejects strings by default.
        /// </summary>
        public static MovieFileVariant Parse(string? value)
        {
            return (value ?? "").Trim().ToLowerInvariant() switch
            {
                "hd" => MovieFileVariant.Hd,
                "slim" or "proxy" => MovieFileVariant.Slim,
                _ => MovieFileVariant.Unknown
            };
        }
    }

    public sealed class MovieFolderDto
    {
        public string Prefix { get; set; } = "";
        public string Name { get; set; } = "";
    }

    public sealed class MovieFileDto
    {
        public string Key { get; set; } = "";
        public string Name { get; set; } = "";
        public long SizeBytes { get; set; }
        public DateTime? LastModified { get; set; }
        public string Variant { get; set; } = "unknown";
    }

    public sealed class MovieFileListResponse
    {
        public string Bucket { get; set; } = "";
        public string Prefix { get; set; } = "";
        public string ParentPrefix { get; set; } = "";
        public List<MovieFolderDto> Folders { get; set; } = new();
        public List<MovieFileDto> Files { get; set; } = new();
        public string? NextToken { get; set; }
        public string StagingRoot { get; set; } = "";
    }

    public sealed class CreateFolderRequest
    {
        public string Prefix { get; set; } = "";
    }

    public sealed class UploadUrlRequest
    {
        public string Prefix { get; set; } = "";
        public string FileName { get; set; } = "";

        /// <summary>"hd", "slim" or empty to keep the file name as-is.</summary>
        public string? Variant { get; set; }

        public string? ContentType { get; set; }
        public long SizeBytes { get; set; }
    }

    public sealed class PresignedUploadResponse
    {
        public string Key { get; set; } = "";
        public string Url { get; set; } = "";
        public string ContentType { get; set; } = "";

        /// <summary>True when a file already exists at this key.</summary>
        public bool Overwrites { get; set; }
    }

    public sealed class MultipartUploadResponse
    {
        public string Key { get; set; } = "";
        public string UploadId { get; set; } = "";
        public int PartSizeBytes { get; set; }
        public string ContentType { get; set; } = "";
        public bool Overwrites { get; set; }
    }

    public sealed class PartUrlsRequest
    {
        public string Key { get; set; } = "";
        public string UploadId { get; set; } = "";
        public List<int> PartNumbers { get; set; } = new();
    }

    public sealed class PresignedPart
    {
        public int PartNumber { get; set; }
        public string Url { get; set; } = "";
    }

    public sealed class MultipartPart
    {
        public int PartNumber { get; set; }
        public string ETag { get; set; } = "";
    }

    public sealed class CompleteMultipartRequest
    {
        public string Key { get; set; } = "";
        public string UploadId { get; set; } = "";
        public List<MultipartPart> Parts { get; set; } = new();
    }

    public sealed class AbortMultipartRequest
    {
        public string Key { get; set; } = "";
        public string UploadId { get; set; } = "";
    }

    public sealed class RenameObjectRequest
    {
        public string Key { get; set; } = "";
        public string NewName { get; set; } = "";
    }

    public sealed class MovePrefixRequest
    {
        public string SourcePrefix { get; set; } = "";
        public string TargetPrefix { get; set; } = "";
        public bool DeleteSource { get; set; } = true;
    }

    /// <summary>Start an SF proxy encode of the HD master at Key.</summary>
    public sealed class CreateTranscodeJobRequest
    {
        public string Key { get; set; } = "";
        public string? Preset { get; set; }
        public bool Overwrite { get; set; }
    }

    public sealed class MovePrefixResponse
    {
        public string SourcePrefix { get; set; } = "";
        public string TargetPrefix { get; set; } = "";
        public int FilesMoved { get; set; }
        public long BytesMoved { get; set; }
        public bool SourceDeleted { get; set; }
    }
}
