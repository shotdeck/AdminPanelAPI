namespace AdminPanelAPI.Models
{
    public class ClipPreviewBoundary
    {
        public int MovieId { get; set; }
        public string Filename { get; set; } = "";
        public double StartTime { get; set; }
        public double EndTime { get; set; }
        public int ImageId { get; set; }
    }

    public class ClipPreviewResult
    {
        public int MovieId { get; set; }
        public int Requested { get; set; }
        public int Created { get; set; }
        public int Exists { get; set; }
        public int Skipped { get; set; }
        public int Errors { get; set; }
    }

    public class ClipPreviewBackfillResult
    {
        public int MoviesProcessed { get; set; }
        public int Requested { get; set; }
        public int Created { get; set; }
        public int Exists { get; set; }
        public int Skipped { get; set; }
        public int Errors { get; set; }
        public int? NextAfterMovieId { get; set; }
        public List<ClipPreviewResult> Movies { get; set; } = new();
    }

    public class ClipPreviewMotionResult
    {
        public int Requested { get; set; }
        public int Created { get; set; }
        public int Exists { get; set; }
        public int Skipped { get; set; }
        public int Errors { get; set; }
        public int? NextAfterImageId { get; set; }
    }

    public class ClipPreviewMotionCounts
    {
        public long Total { get; set; }
        public long AtOrBeforeCursor { get; set; }
        public int? FirstImageId { get; set; }
        public int? LastImageId { get; set; }
    }

    public class ClipPreviewProgress
    {
        public int? MovieId { get; set; }

        /// <summary>Motion-tagged clips eligible for a preview.</summary>
        public long MotionTaggedTotal { get; set; }

        /// <summary>Eligible clips the cursor has already passed.</summary>
        public long PassedCursor { get; set; }
        public long RemainingAfterCursor { get; set; }
        public double PercentPassedCursor { get; set; }
        public int AfterImageId { get; set; }
        public int? LastEligibleImageId { get; set; }

        /// <summary>
        /// Preview objects in R2. Counts every preview, including ones made by the
        /// movie-level backfill for clips without a motion tag, so it is not a
        /// subset of MotionTaggedTotal.
        /// </summary>
        public long? PreviewsInR2 { get; set; }
        public bool PreviewCountTruncated { get; set; }
        public double? PreviewCountSeconds { get; set; }
    }

    /// <summary>
    /// State of the unattended motion preview run, which walks every eligible clip
    /// in batches until none are left.
    /// </summary>
    public class ClipPreviewRunStatus
    {
        public bool Running { get; set; }
        public bool StopRequested { get; set; }
        public DateTime? StartedAtUtc { get; set; }
        public DateTime? FinishedAtUtc { get; set; }
        public double? ElapsedMinutes { get; set; }

        public int StartedAfterImageId { get; set; }
        public int BatchSize { get; set; }
        public bool Overwrite { get; set; }

        /// <summary>Cursor the next batch will start after.</summary>
        public int Cursor { get; set; }

        public int Batches { get; set; }
        public int Requested { get; set; }
        public int Created { get; set; }
        public int Exists { get; set; }
        public int Skipped { get; set; }
        public int Errors { get; set; }

        /// <summary>Set when the run reached the end of the eligible clips.</summary>
        public bool CompletedAll { get; set; }

        /// <summary>Last failure, if any. The run keeps going after one.</summary>
        public string? LastError { get; set; }
        public DateTime? LastErrorAtUtc { get; set; }
    }

    public class ClipPreviewBatchResponse
    {
        public int Total { get; set; }
        public Dictionary<string, int>? Summary { get; set; }
    }
}
