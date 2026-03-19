namespace ShotDeckSearch.Models
{
    public class KeywordCacheStatus
    {
        public DateTimeOffset? LastCsvWarmAt { get; init; }
        public DateTimeOffset? LastRefreshStartAt { get; init; }
        public DateTimeOffset? LastRefreshEndAt { get; init; }
        public bool IsRefreshRunning { get; init; }
        public bool LastRefreshSucceeded { get; init; }
        public string? LastRefreshError { get; init; }
        public int FlatCount { get; init; }
        public int SourceKeys { get; init; }
        public int CategoryCount { get; init; }
    }
}
