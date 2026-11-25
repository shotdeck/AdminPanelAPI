using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Drops: the releases titles are tagged for. A drop has a drop date and a
    /// due date, a title belongs to at most one drop and takes its due date
    /// unless an admin has given that title its own, and a title can be marked
    /// priority. Settings hold the few numbers the tagger hub works to.
    /// </summary>
    public static class TaggingDropStore
    {
        public const string MaxTitlesPerTagger = "max_titles_per_tagger";
        public const string OpenDropWeeks = "open_drop_weeks";
        public const string DueDaysBeforeDrop = "due_days_before_drop";

        public static readonly IReadOnlyDictionary<string, int> DefaultSettings =
            new Dictionary<string, int>
            {
                [MaxTitlesPerTagger] = 3,
                [OpenDropWeeks] = 2,
                [DueDaysBeforeDrop] = 5
            };

        /// <summary>Mirrors migrations/061.</summary>
        public const string Schema = @"
CREATE TABLE IF NOT EXISTS frl.frl_tagging_drops (
    id          BIGSERIAL    PRIMARY KEY,
    drop_date   DATE         NOT NULL UNIQUE,
    due_date    DATE         NOT NULL,
    note        TEXT,
    created_by  VARCHAR(120),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS frl.frl_tagging_drop_titles (
    movie_id           INTEGER      PRIMARY KEY,
    drop_id            BIGINT       NOT NULL
        REFERENCES frl.frl_tagging_drops (id) ON DELETE CASCADE,
    priority           BOOLEAN      NOT NULL DEFAULT false,
    due_date_override  DATE,
    updated_by         VARCHAR(120),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ftdt_drop
    ON frl.frl_tagging_drop_titles (drop_id);
CREATE TABLE IF NOT EXISTS frl.frl_tagging_settings (
    name        VARCHAR(64)  PRIMARY KEY,
    value       INTEGER      NOT NULL,
    updated_by  VARCHAR(120),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
INSERT INTO frl.frl_tagging_settings (name, value) VALUES
    ('max_titles_per_tagger', 3),
    ('open_drop_weeks', 2),
    ('due_days_before_drop', 5)
ON CONFLICT (name) DO NOTHING;";

        /// <summary>
        /// The join that gives an assignment query its drop: alias t for the
        /// title's row and d for its drop, both optional.
        /// </summary>
        public const string AssignmentJoin = @"
LEFT JOIN frl.frl_tagging_drop_titles t ON t.movie_id = a.movie_id
LEFT JOIN frl.frl_tagging_drops d ON d.id = t.drop_id";

        /// <summary>The drop date, due date and priority read with <see cref="AssignmentJoin"/>, in order.</summary>
        public const string AssignmentColumns =
            "d.drop_date, COALESCE(t.due_date_override, d.due_date), COALESCE(t.priority, false)";

        public static string Format(DateOnly? date) => date?.ToString("yyyy-MM-dd") ?? "";

        public static DateOnly? ReadDate(NpgsqlDataReader reader, int index) =>
            reader.IsDBNull(index) ? null : reader.GetFieldValue<DateOnly>(index);

        /// <summary>A yyyy-MM-dd date from a request, or null when it isn't one.</summary>
        public static DateOnly? Parse(string? value) =>
            DateOnly.TryParseExact((value ?? "").Trim(), "yyyy-MM-dd", out var date) ? date : null;

        /// <summary>A drop's due date when none is given: the set number of days before it.</summary>
        public static DateOnly DefaultDue(DateOnly dropDate, int daysBefore) =>
            dropDate.AddDays(-Math.Max(0, daysBefore));

        public static async Task<Dictionary<string, int>> SettingsAsync(
            NpgsqlConnection connection, CancellationToken ct)
        {
            var settings = new Dictionary<string, int>(DefaultSettings);
            await using var cmd = new NpgsqlCommand(
                "SELECT name, value FROM frl.frl_tagging_settings;", connection);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                settings[reader.GetString(0)] = reader.GetInt32(1);
            return settings;
        }
    }
}
