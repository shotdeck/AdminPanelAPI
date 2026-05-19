namespace AdminPanelAPI.Models
{
    public class CaptionEmbeddingJobStatusResponse
    {
        public long JobId { get; set; }
        public int BatchSize { get; set; }

        public string Status { get; set; } = "";
        public string? CurrentStep { get; set; }

        public int? ProgressCurrent { get; set; }
        public int? ProgressTotal { get; set; }

        public DateTime CreatedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public string? Error { get; set; }
    }
}
