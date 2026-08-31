namespace AdminPanelAPI.Models
{
    public class MovieMissingClipSummary
    {
        public int MovieId { get; set; }

        public int LiveImages { get; set; }

        /// <summary>
        /// Live images of this movie with no scene boundary row.
        /// </summary>
        public int MissingBoundaries { get; set; }

        /// <summary>
        /// False when the movie has never completed the pipeline, in which case it
        /// is a candidate for a full run rather than a missing-clips rerun.
        /// </summary>
        public bool HasCompletedJob { get; set; }
    }

    public class MovieMissingClipSummaryResponse
    {
        public int TotalMovies { get; set; }
        public long TotalMissingBoundaries { get; set; }

        public List<MovieMissingClipSummary> Movies { get; set; } = new();
    }
}
