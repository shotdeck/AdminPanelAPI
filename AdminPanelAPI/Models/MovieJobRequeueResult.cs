namespace AdminPanelAPI.Models
{
    public class MovieJobRequeueResult
    {
        public bool DryRun { get; set; }

        /// <summary>Failed jobs selected.</summary>
        public int Failed { get; set; }

        /// <summary>Running jobs whose start is older than the staleness cut-off.</summary>
        public int StaleRunning { get; set; }

        public int Requeued { get; set; }

        public List<long> JobIds { get; set; } = new();

        /// <summary>The selected failures grouped by message, so a repeating cause is obvious before retrying.</summary>
        public List<MovieJobRequeueErrorGroup> Errors { get; set; } = new();
    }

    public class MovieJobRequeueErrorGroup
    {
        public string Error { get; set; } = string.Empty;
        public int Count { get; set; }
    }
}
