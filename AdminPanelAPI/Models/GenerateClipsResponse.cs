using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    public class GenerateClipsResponse
    {
        [JsonPropertyName("movie_id")]
        public int MovieId { get; set; }

        [JsonPropertyName("message")]
        public string? Message { get; set; }

        [JsonPropertyName("total_frames")]
        public int? TotalFrames { get; set; }

        [JsonPropertyName("processed_frames")]
        public int? ProcessedFrames { get; set; }

        [JsonPropertyName("skipped_download")]
        public bool? SkippedDownload { get; set; }

        /// <summary>The Modal generator answers 200 with this set when it fails.</summary>
        [JsonPropertyName("error")]
        public string? Error { get; set; }
    }
}
