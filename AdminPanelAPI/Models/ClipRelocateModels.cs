using System.Text.Json;
using System.Text.Json.Serialization;

namespace AdminPanelAPI.Models
{
    public class ClipRelocateJob
    {
        public long Id { get; set; }
        public int MovieId { get; set; }
        public bool Apply { get; set; }
        public bool WriteHistory { get; set; }
        public string Status { get; set; } = "";
        public string? ModalCallId { get; set; }
        public string? Phase { get; set; }
        public JsonElement? Progress { get; set; }
        public int Images { get; set; }
        public int Flagged { get; set; }
        public int Relocated { get; set; }
        public int NotFound { get; set; }
        public int Failed { get; set; }
        public JsonElement? Report { get; set; }
        public string? Error { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
        public DateTime? FinishedAt { get; set; }
    }

    /// <summary>One live image of a relocate job's movie, as sent to Modal.</summary>
    public class ClipRelocateImage
    {
        [JsonPropertyName("rid")] public string Rid { get; set; } = "";
        [JsonPropertyName("frame")] public int? Frame { get; set; }
        [JsonIgnore] public int ImageId { get; set; }
    }

    /// <summary>One image's outcome from the clip-relocate Modal app.</summary>
    public class ClipRelocateImageResult
    {
        [JsonPropertyName("rid")] public string Rid { get; set; } = "";
        /// <summary>ok | flagged | relocated | not_found | failed | no_still</summary>
        [JsonPropertyName("status")] public string Status { get; set; } = "";
        [JsonPropertyName("reason")] public string? Reason { get; set; }
        [JsonPropertyName("history_frame")] public int? HistoryFrame { get; set; }
        [JsonPropertyName("best_t")] public double? BestTime { get; set; }
        [JsonPropertyName("best")] public double? BestScore { get; set; }
        [JsonPropertyName("centre")] public double? CentreScore { get; set; }
        [JsonPropertyName("found_time")] public double? FoundTime { get; set; }
        [JsonPropertyName("found_frame")] public int? FoundFrame { get; set; }
        [JsonPropertyName("found_score")] public double? FoundScore { get; set; }
        [JsonPropertyName("recut_score")] public double? RecutScore { get; set; }
    }

    public class ClipRelocateModalResult
    {
        [JsonPropertyName("movie_id")] public int MovieId { get; set; }
        [JsonPropertyName("apply")] public bool Apply { get; set; }
        [JsonPropertyName("backup_prefix")] public string? BackupPrefix { get; set; }
        [JsonPropertyName("images")] public List<ClipRelocateImageResult> Images { get; set; } = new();
    }

    public class ClipRelocatePoll
    {
        [JsonPropertyName("status")] public string Status { get; set; } = "";
        [JsonPropertyName("progress")] public JsonElement? Progress { get; set; }
        [JsonPropertyName("result")] public ClipRelocateModalResult? Result { get; set; }
        [JsonPropertyName("error")] public string? Error { get; set; }
    }
}
