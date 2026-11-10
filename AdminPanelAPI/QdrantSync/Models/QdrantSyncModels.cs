namespace AdminPanelAPI.QdrantSync.Models;

/// <summary>Settings under <c>QdrantSync</c>.</summary>
public sealed class QdrantSyncOptions
{
    public string Url { get; set; } = "";
    public string ApiKey { get; set; } = "";
    public string Collection { get; set; } = "images";
    /// <summary>Optional: read frl tables from here instead of ConnectionStrings:Default. Jobs always live in Default.</summary>
    public string? SourceConnectionString { get; set; }
    public int BatchSize { get; set; } = 2000;
    public int WriteBatchSize { get; set; } = 100;
    public bool IncludeColorShades { get; set; }
    public int DbTimeoutSeconds { get; set; } = 180;
    public int LogEveryRows { get; set; } = 10000;
    public int MaxLoggedRecords { get; set; } = 200;
    public string VectorName { get; set; } = "qwen3";
    /// <summary>Second named vector (image similarity), from <c>frl_cinemanet_embeddings</c> joined on randid.</summary>
    public string CinemanetVectorName { get; set; } = "cinemanet";
    public int CinemanetVectorSize { get; set; } = 1280;
    public string CinemanetDistance { get; set; } = "Cosine";
    public bool CinemanetOnDisk { get; set; } = true;
    /// <summary>Rebuild job: new collection (qwen3 + cinemanet) that <see cref="Collection"/> is copied into.</summary>
    public string RebuildTargetCollection { get; set; } = "images_v2";
    /// <summary>Rebuild job: points copied per scroll page.</summary>
    public int RebuildBatchSize { get; set; } = 250;
    /// <summary>Insert job: images loaded (with vectors) and upserted per request.</summary>
    public int InsertBatchSize { get; set; } = 500;
    /// <summary>Insert job: embedded ids read from Postgres per query when looking for missing ones.</summary>
    public int IdScanBatchSize { get; set; } = 20000;
    public int ScrollPageSize { get; set; } = 10000;
    public NightlyOptions Nightly { get; set; } = new();
    public EmailOptions Email { get; set; } = new();

    public bool QdrantConfigured => !string.IsNullOrWhiteSpace(Url) && !string.IsNullOrWhiteSpace(ApiKey);

    public sealed class NightlyOptions
    {
        public bool Enabled { get; set; }
        /// <summary>UTC time of day, HH:mm. 09:00 UTC is 02:00 Mountain Standard Time.</summary>
        public string AtUtc { get; set; } = "09:00";
        public bool Apply { get; set; } = true;
        /// <summary>Also queue the insert-missing job each night, ahead of the sync.</summary>
        public bool Insert { get; set; } = true;
        /// <summary>A nightly run that couldn't start on time (another job running, API down) may still start this many hours late.</summary>
        public int WindowHours { get; set; } = 6;
    }

    public sealed class EmailOptions
    {
        public string SmtpHost { get; set; } = "smtp.gmail.com";
        public int SmtpPort { get; set; } = 587;
        public string Username { get; set; } = "";
        public string Password { get; set; } = "";
        public string From { get; set; } = "";
        /// <summary>Comma-separated.</summary>
        public string To { get; set; } = "";

        public bool Configured => !string.IsNullOrWhiteSpace(SmtpHost) && !string.IsNullOrWhiteSpace(Password)
                                  && !string.IsNullOrWhiteSpace(To);
    }
}

public sealed class QdrantSyncJob
{
    public long Id { get; set; }
    /// <summary>sync (update payloads of existing points) | insert (add embedded images missing from Qdrant)</summary>
    public string Kind { get; set; } = "sync";
    public bool Apply { get; set; }
    public string TriggeredBy { get; set; } = "manual";
    public DateOnly? NightlyDate { get; set; }
    /// <summary>queued | running | done | failed | cancelled</summary>
    public string Status { get; set; } = "queued";
    public int StartAfterId { get; set; }
    public long MaxRows { get; set; }
    public bool Email { get; set; }
    public int LastId { get; set; }
    public long Scanned { get; set; }
    /// <summary>Sync: live images that are in Qdrant. Insert: embedded images already in Qdrant.</summary>
    public long Found { get; set; }
    /// <summary>Sync: live images not in Qdrant (left for the insert job). Insert: embedded images missing from Qdrant.</summary>
    public long Missing { get; set; }
    public long Changed { get; set; }
    public long Unchanged { get; set; }
    public long Written { get; set; }
    public Dictionary<string, long> FieldCounts { get; set; } = new();
    /// <summary>Field-level before/after for the first changed records (MaxLoggedRecords). Only on a single-job read.</summary>
    public List<ChangeExample>? Examples { get; set; }
    public int Retries { get; set; }
    public string? EmailStatus { get; set; }
    public string? Error { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime UpdatedAt { get; set; }
    public DateTime? FinishedAt { get; set; }
}

public sealed record ChangeExample(long ImageId, string Field, string Qdrant, string Postgres);
