using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    public class SegmentManifestEntry
    {
        [JsonPropertyName("index")]
        public int Index { get; set; }

        [JsonPropertyName("start_time")]
        public double StartTime { get; set; }

        [JsonPropertyName("duration")]
        public double Duration { get; set; }

        [JsonPropertyName("r2_key")]
        public string R2Key { get; set; } = "";
    }

    /// <summary>
    /// Response from the Modal /segment endpoint after splitting a movie into
    /// ~10s stream-copied segments in R2.
    /// </summary>
    public class SegmentManifestResponse
    {
        [JsonPropertyName("movie_id")]
        public int MovieId { get; set; }

        [JsonPropertyName("segment_count")]
        public int SegmentCount { get; set; }

        [JsonPropertyName("total_duration")]
        public double TotalDuration { get; set; }

        [JsonPropertyName("segments")]
        public List<SegmentManifestEntry> Segments { get; set; } = new();

        [JsonPropertyName("detail")]
        public string? Detail { get; set; }
    }
}
