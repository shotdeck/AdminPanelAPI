using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    public class VideoInfoResponse
    {
        [JsonPropertyName("duration")]
        public double? Duration { get; set; }

        [JsonPropertyName("fps")]
        public double? Fps { get; set; }

        [JsonPropertyName("frame_count")]
        public int? FrameCount { get; set; }

        [JsonPropertyName("width")]
        public int? Width { get; set; }

        [JsonPropertyName("height")]
        public int? Height { get; set; }

        [JsonPropertyName("aspect_ratio")]
        public double? AspectRatio { get; set; }

        [JsonPropertyName("aspect_ratio_str")]
        public string? AspectRatioStr { get; set; }

        [JsonPropertyName("dar")]
        public string? Dar { get; set; }

        [JsonPropertyName("sar")]
        public string? Sar { get; set; }

        [JsonPropertyName("codec")]
        public string? Codec { get; set; }

        [JsonPropertyName("profile")]
        public string? Profile { get; set; }

        [JsonPropertyName("pix_fmt")]
        public string? PixFmt { get; set; }

        [JsonPropertyName("bit_depth")]
        public int? BitDepth { get; set; }

        [JsonPropertyName("file_size_bytes")]
        public long? FileSizeBytes { get; set; }

        [JsonPropertyName("file_size_mb")]
        public double? FileSizeMb { get; set; }
    }
}
