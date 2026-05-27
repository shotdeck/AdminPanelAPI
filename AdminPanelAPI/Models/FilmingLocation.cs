namespace AdminPanelAPI.Models
{
    public sealed class FilmingLocationDto
    {
        public long Id { get; set; }
        public int ImageId { get; set; }
        public string? RawLocation { get; set; }
        public string? Planet { get; set; }
        public string? Continent { get; set; }
        public string? Country { get; set; }
        public string? StateRegion { get; set; }
        public string? City { get; set; }
        public string? SpecificLocation { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
    }

    public sealed class FilmingLocationCreateDto
    {
        public int ImageId { get; set; }
        public string? Planet { get; set; }
        public string? Continent { get; set; }
        public string? Country { get; set; }
        public string? StateRegion { get; set; }
        public string? City { get; set; }
        public string? SpecificLocation { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
    }

    public sealed class FilmingLocationUpdateDto
    {
        public string? Planet { get; set; }
        public string? Continent { get; set; }
        public string? Country { get; set; }
        public string? StateRegion { get; set; }
        public string? City { get; set; }
        public string? SpecificLocation { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
    }

    public sealed class ParseProgressResponse
    {
        public int TotalImages { get; set; }
        public int Processed { get; set; }
        public int Skipped { get; set; }
        public int Failed { get; set; }
        public string Status { get; set; } = string.Empty;
    }
}
