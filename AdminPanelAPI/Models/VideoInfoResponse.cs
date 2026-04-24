namespace AdminPanelAPI.Models
{
    public class VideoInfoResponse
    {
        public double? Duration { get; set; }
        public double? Fps { get; set; }
        public int? FrameCount { get; set; }
        public int? Width { get; set; }
        public int? Height { get; set; }
        public double? AspectRatio { get; set; }
        public string? AspectRatioStr { get; set; }
        public string? Dar { get; set; }
        public string? Sar { get; set; }
        public string? Codec { get; set; }
        public string? Profile { get; set; }
        public string? PixFmt { get; set; }
        public int? BitDepth { get; set; }
        public long? FileSizeBytes { get; set; }
        public double? FileSizeMb { get; set; }
    }
}
