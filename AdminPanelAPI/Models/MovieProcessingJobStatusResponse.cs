namespace AdminPanelAPI.Models
{
    public class MovieProcessingJobStatusResponse
    {
        public long JobId { get; set; }
        public int MovieId { get; set; }
        public double Threshold { get; set; }
        public bool Overwrite { get; set; }
        public bool MissingOnly { get; set; }

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
