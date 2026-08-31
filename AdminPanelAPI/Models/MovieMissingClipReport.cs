namespace AdminPanelAPI.Models
{
    public class MovieMissingClipReport
    {
        public int MovieId { get; set; }

        public int LiveImages { get; set; }
        public int ClipsInR2 { get; set; }
        public int SceneBoundaries { get; set; }

        /// <summary>
        /// Clips present in R2 that have no scene boundary row yet.
        /// </summary>
        public List<string> ClipsWithoutBoundary { get; set; } = new();

        /// <summary>
        /// Live images whose 9-second clip has never been generated into R2.
        /// </summary>
        public List<string> ImagesWithoutClip { get; set; } = new();
    }
}
