namespace AdminPanelAPI.Models
{
    public class ImageRecord
    {
        public int Id { get; set; }
        public int? MovieId { get; set; }
        public string? Filename { get; set; }
        public string? Randid { get; set; }
        public string? Format { get; set; }
        public string? OpticalFormat { get; set; }
        public string? TimePeriod { get; set; }
        public string? Setting { get; set; }
        public string? Location { get; set; }
        public string? FilmingLocation { get; set; }
        public string? Actors { get; set; }
        public string? IntExt { get; set; }
    }
}
