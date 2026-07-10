using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    /// <summary>
    /// Response from POST /identify — the spawned call id to poll for results.
    /// </summary>
    public class MusicApiStartResponse
    {
        [JsonPropertyName("call_id")]
        public string? CallId { get; set; }

        [JsonPropertyName("status")]
        public string? Status { get; set; }
    }

    public class MusicApiResponse
    {
        [JsonPropertyName("status")]
        public string? Status { get; set; }

        [JsonPropertyName("movie_id")]
        public int MovieId { get; set; }

        [JsonPropertyName("duration")]
        public double Duration { get; set; }

        [JsonPropertyName("window_count")]
        public int WindowCount { get; set; }

        [JsonPropertyName("matched_segments")]
        public List<MatchedSegment> MatchedSegments { get; set; } = new();

        [JsonPropertyName("unmatched_windows")]
        public List<UnmatchedWindow> UnmatchedWindows { get; set; } = new();

        [JsonPropertyName("error")]
        public string? Error { get; set; }
    }

    public class MatchedSegment
    {
        [JsonPropertyName("start")]
        public double Start { get; set; }

        [JsonPropertyName("end")]
        public double End { get; set; }

        [JsonPropertyName("title")]
        public string? Title { get; set; }

        [JsonPropertyName("artist")]
        public string? Artist { get; set; }

        [JsonPropertyName("recording_id")]
        public string? RecordingId { get; set; }

        [JsonPropertyName("isrc")]
        public string? Isrc { get; set; }

        [JsonPropertyName("score")]
        public double? Score { get; set; }

        [JsonPropertyName("source")]
        public string? Source { get; set; }

        [JsonPropertyName("spotify_url")]
        public string? SpotifyUrl { get; set; }
    }

    public class UnmatchedWindow
    {
        [JsonPropertyName("start")]
        public double Start { get; set; }

        [JsonPropertyName("end")]
        public double End { get; set; }
    }
}
