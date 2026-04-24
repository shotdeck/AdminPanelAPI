using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    public class SceneBoundaryResponse
    {
        [JsonPropertyName("movie_id")]
        public int MovieId { get; set; }

        [JsonPropertyName("filename")]
        public string? Filename { get; set; }

        [JsonPropertyName("threshold")]
        public double? Threshold { get; set; }

        [JsonPropertyName("start_time")]
        public double? StartTime { get; set; }

        [JsonPropertyName("end_time")]
        public double? EndTime { get; set; }

        [JsonPropertyName("cut_times")]
        public List<double>? CutTimes { get; set; }

        [JsonPropertyName("duration")]
        public double? Duration { get; set; }

        [JsonPropertyName("fps")]
        public double? Fps { get; set; }

        [JsonPropertyName("frame_count")]
        public int? FrameCount { get; set; }

        [JsonPropertyName("target_frame")]
        public int? TargetFrame { get; set; }
    }
}
