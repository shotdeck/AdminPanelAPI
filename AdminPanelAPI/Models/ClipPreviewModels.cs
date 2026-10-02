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

    public class ClipPreviewBatchResponse
    {
        public int Total { get; set; }
        public Dictionary<string, int>? Summary { get; set; }
    }
}
