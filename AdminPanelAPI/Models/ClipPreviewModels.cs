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

    public class ClipPreviewBatchResponse
    {
        public int Total { get; set; }
        public Dictionary<string, int>? Summary { get; set; }
    }
}
