namespace AdminPanelAPI.VideoLinkCheck.Models;

/// <summary>Settings under <c>VideoLinkCheck</c>.</summary>
public sealed class VideoLinkCheckOptions
{
    /// <summary>Comma-separated frl_movies.media_type values to check (case-insensitive).</summary>
    public string MediaTypes { get; set; } = "music video,commercial";
    /// <summary>Only movies with at least this many live frl_images are checked.</summary>
    public int MinLiveImages { get; set; } = 2;
    /// <summary>Movies read, checked and saved per checkpoint.</summary>
    public int BatchSize { get; set; } = 250;
    public int MaxConcurrent { get; set; } = 12;
    public int MaxRetries { get; set; } = 2;
    public int RequestTimeoutSeconds { get; set; } = 15;
    public int DbTimeoutSeconds { get; set; } = 300;
    /// <summary>Used for the CSV download link in the email.</summary>
    public string PublicBaseUrl { get; set; } = "https://shotdeckadminpanelapi.azurewebsites.net";
    public EmailOptions Email { get; set; } = new();

    public string[] MediaTypeList => MediaTypes
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(t => t.ToLowerInvariant()).Distinct().ToArray();

    /// <summary>Blank SMTP fields fall back to QdrantSync:Email, so normally only To needs setting.</summary>
    public sealed class EmailOptions
    {
        public string SmtpHost { get; set; } = "";
        public int SmtpPort { get; set; }
        public string Username { get; set; } = "";
        public string Password { get; set; } = "";
        public string From { get; set; } = "";
        /// <summary>Default recipients, comma-separated. A run's <c>to</c> overrides it.</summary>
        public string To { get; set; } = "";
    }
}

public sealed class VideoLinkCheckJob
{
    public long Id { get; set; }
    /// <summary>queued | running | done | failed | cancelled</summary>
    public string Status { get; set; } = "queued";
    public string TriggeredBy { get; set; } = "manual";
    public bool Email { get; set; }
    /// <summary>Recipients for this run; null = VideoLinkCheck:Email:To.</summary>
    public string? EmailTo { get; set; }
    public long LastId { get; set; }
    public long Scanned { get; set; }
    public long Active { get; set; }
    public long Unavailable { get; set; }
    public long UnknownPlatform { get; set; }
    public long Errors { get; set; }
    /// <summary>Unavailable now but active in <see cref="PreviousJobId"/>.</summary>
    public long NewlyUnavailable { get; set; }
    /// <summary>The last finished check before this one, compared against at the end.</summary>
    public long? PreviousJobId { get; set; }
    public string? EmailStatus { get; set; }
    public string? Error { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
}

public sealed record MovieLink(long Id, string Url, string MediaType, string Title);

/// <summary>One movie's check. Status: active | unavailable | unknown_platform | error.</summary>
public sealed record LinkCheckResult(
    long MovieId, string Title, string MediaType, string Url, string Platform, string VideoId,
    string Status, DateTime CheckedAt, string? Note, string? PreviousStatus = null);
