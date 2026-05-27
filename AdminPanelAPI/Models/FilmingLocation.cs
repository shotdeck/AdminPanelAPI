namespace AdminPanelAPI.Models
{
    // ── Lookup table DTOs ────────────────────────────────────────

    public sealed class ContinentDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class CountryDto
    {
        public int Id { get; set; }
        public int? ContinentId { get; set; }
        public string? ContinentName { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class RegionDto
    {
        public int Id { get; set; }
        public int? CountryId { get; set; }
        public string? CountryName { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public sealed class CityDto
    {
        public int Id { get; set; }
        public int? RegionId { get; set; }
        public string? RegionName { get; set; }
        public int? CountryId { get; set; }
        public string? CountryName { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // ── Alias DTOs ───────────────────────────────────────────────

    public sealed class AliasDto
    {
        public int Id { get; set; }
        public string Alias { get; set; } = string.Empty;
        public int CanonicalId { get; set; }
        public string CanonicalName { get; set; } = string.Empty;
    }

    public sealed class CreateAliasDto
    {
        public string Alias { get; set; } = string.Empty;
        public int CanonicalId { get; set; }
    }

    // ── Image location DTOs ──────────────────────────────────────

    public sealed class FilmingLocationDto
    {
        public long Id { get; set; }
        public int ImageId { get; set; }
        public string? RawLocation { get; set; }
        public int? ContinentId { get; set; }
        public string? ContinentName { get; set; }
        public int? CountryId { get; set; }
        public string? CountryName { get; set; }
        public int? RegionId { get; set; }
        public string? RegionName { get; set; }
        public int? CityId { get; set; }
        public string? CityName { get; set; }
        public string? SpecificLocation { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public float Confidence { get; set; }
        public bool NeedsReview { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
    }

    public sealed class FilmingLocationCreateDto
    {
        public int ImageId { get; set; }
        public int? ContinentId { get; set; }
        public int? CountryId { get; set; }
        public int? RegionId { get; set; }
        public int? CityId { get; set; }
        public string? SpecificLocation { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
    }

    public sealed class FilmingLocationUpdateDto
    {
        public int? ContinentId { get; set; }
        public int? CountryId { get; set; }
        public int? RegionId { get; set; }
        public int? CityId { get; set; }
        public string? SpecificLocation { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public bool? NeedsReview { get; set; }
    }

    public sealed class ParseProgressResponse
    {
        public int TotalImages { get; set; }
        public int Processed { get; set; }
        public int Skipped { get; set; }
        public int FuzzyMatched { get; set; }
        public int FlaggedForReview { get; set; }
        public int Failed { get; set; }
        public string Status { get; set; } = string.Empty;
    }

    public sealed class CoordinatesDto
    {
        public double Latitude { get; set; }
        public double Longitude { get; set; }
    }

    public sealed class FilmingLocationPageResponse
    {
        public int TotalCount { get; set; }
        public int Page { get; set; }
        public int PageSize { get; set; }
        public List<FilmingLocationDto> Items { get; set; } = new();
    }

    public sealed class FilmingLocationStats
    {
        public long TotalLocations { get; set; }
        public long GeocodedCount { get; set; }
        public long NeedsReviewCount { get; set; }
        public long DistinctCountries { get; set; }
        public long DistinctCities { get; set; }
        public long DistinctImages { get; set; }
    }

    public sealed class ReviewItemDto
    {
        public long Id { get; set; }
        public int ImageId { get; set; }
        public string? RawLocation { get; set; }
        public string? ContinentName { get; set; }
        public string? CountryName { get; set; }
        public string? RegionName { get; set; }
        public string? CityName { get; set; }
        public string? SpecificLocation { get; set; }
        public float Confidence { get; set; }
    }
}
