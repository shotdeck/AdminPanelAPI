namespace AdminPanelAPI.Models
{
    public class MusicIdentificationJobStatusResponse
    {
        public long JobId { get; set; }
        public int MovieId { get; set; }

        public string Status { get; set; } = "";
        public string? CurrentStep { get; set; }
        public int ProgressPct { get; set; }
        public int? MatchedCount { get; set; }
        public int? UnmatchedCount { get; set; }

        public string? R2Key { get; set; }
        public string? R2Url { get; set; }

        public DateTime CreatedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public string? Error { get; set; }

        /// <summary>
        /// A non-fatal problem during a job that still completed — e.g. music
        /// was identified but AI descriptions couldn't be generated because the
        /// OpenAI quota was exceeded. Shown to the user without marking the job
        /// as failed.
        /// </summary>
        public string? Warning { get; set; }
    }
}
