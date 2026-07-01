using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    public class TranscriptionWord
    {
        [JsonPropertyName("word")]
        public string Word { get; set; } = "";

        [JsonPropertyName("start")]
        public double Start { get; set; }

        [JsonPropertyName("end")]
        public double End { get; set; }

        [JsonPropertyName("probability")]
        public double? Probability { get; set; }
    }

    public class TranscriptionApiResponse
    {
        [JsonPropertyName("movie_id")]
        public int MovieId { get; set; }

        [JsonPropertyName("duration")]
        public double? Duration { get; set; }

        [JsonPropertyName("words")]
        public List<TranscriptionWord> Words { get; set; } = new();

        [JsonPropertyName("error")]
        public string? Error { get; set; }
    }
}
