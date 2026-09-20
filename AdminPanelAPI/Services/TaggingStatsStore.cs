using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>A day of tagging work, for the dashboard's charts.</summary>
    public sealed record TaggingDay(
        DateTime Day,
        int Kept,
        int Discarded,
        int Read,
        int Confirmed,
        int Corrections,
        int MoviesFinished);

    /// <summary>What one tagger did over the period asked for.</summary>
    public sealed record TaggerStat(
        string Tagger,
        int MoviesAllocated,
        int MoviesFinished,
        int Kept,
        int Discarded,
        int Confirmed,
        int Corrections,
        DateTime? LastActive);

    /// <summary>How often the model's answer for one category was changed.</summary>
    public sealed record CategoryStat(string Category, int Confirmed, int Corrections);

    /// <summary>Totals over the period, plus where every movie stands today.</summary>
    public sealed record TaggingTotals(
        int Movies,
        int MoviesFinished,
        int Proposed,
        int Kept,
        int Discarded,
        int Read,
        int Confirmed,
        int Corrections,
        int Taggers);

    /// <summary>
    /// The tagging work as numbers: what was proposed, kept, read and stood by,
    /// by whom and on which day. Read-only over the tables the workflow already
    /// writes — nothing here counts anything the pipeline doesn't record.
    /// </summary>
    public static class TaggingStatsStore
    {
        /// <summary>
        /// A frame belongs to the day the tagger touched it, so the series and
        /// the leaderboard agree. `to` is exclusive: the caller passes the day
        /// after the last one wanted.
        /// </summary>
        public static async Task<List<TaggingDay>> DaysAsync(
            NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
        {
            const string sql = @"
WITH days AS (
    SELECT generate_series(@from::date, (@to::date - INTERVAL '1 day'), INTERVAL '1 day')::date AS day
),
decided AS (
    SELECT decided_at::date AS day,
           COUNT(*) FILTER (WHERE decision = 'kept')      AS kept,
           COUNT(*) FILTER (WHERE decision = 'discarded') AS discarded
    FROM frl.frl_movie_key_images
    WHERE decided_at >= @from AND decided_at < @to
    GROUP BY 1
),
read AS (
    SELECT tagged_at::date AS day, COUNT(*) AS n
    FROM frl.frl_movie_key_images
    WHERE tagged_at >= @from AND tagged_at < @to
    GROUP BY 1
),
confirmed AS (
    SELECT tags_confirmed_at::date AS day, COUNT(*) AS n
    FROM frl.frl_movie_key_images
    WHERE tags_confirmed_at >= @from AND tags_confirmed_at < @to
    GROUP BY 1
),
changes AS (
    SELECT changed_at::date AS day, COUNT(*) AS n
    FROM frl.frl_movie_key_image_tag_changes
    WHERE changed_at >= @from AND changed_at < @to
    GROUP BY 1
),
finished AS (
    SELECT tags_confirmed_at::date AS day, COUNT(*) AS n
    FROM frl.frl_movie_tagger_assignments
    WHERE tags_confirmed_at >= @from AND tags_confirmed_at < @to
    GROUP BY 1
)
SELECT d.day,
       COALESCE(dec.kept, 0), COALESCE(dec.discarded, 0),
       COALESCE(r.n, 0), COALESCE(c.n, 0), COALESCE(ch.n, 0), COALESCE(f.n, 0)
FROM days d
LEFT JOIN decided   dec ON dec.day = d.day
LEFT JOIN read      r   ON r.day   = d.day
LEFT JOIN confirmed c   ON c.day   = d.day
LEFT JOIN changes   ch  ON ch.day  = d.day
LEFT JOIN finished  f   ON f.day   = d.day
ORDER BY d.day;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@from", from);
            cmd.Parameters.AddWithValue("@to", to);

            var rows = new List<TaggingDay>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new TaggingDay(
                    reader.GetDateTime(0),
                    reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3),
                    reader.GetInt32(4), reader.GetInt32(5), reader.GetInt32(6)));
            return rows;
        }

        /// <summary>
        /// One row per tagger who either holds a movie or did something in the
        /// period, so an idle tagger still appears with their allocation.
        /// </summary>
        public static async Task<List<TaggerStat>> TaggersAsync(
            NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
        {
            const string sql = @"
WITH decided AS (
    SELECT lower(decided_by) AS who,
           COUNT(*) FILTER (WHERE decision = 'kept')      AS kept,
           COUNT(*) FILTER (WHERE decision = 'discarded') AS discarded,
           MAX(decided_at) AS last_at
    FROM frl.frl_movie_key_images
    WHERE decided_by IS NOT NULL AND decided_at >= @from AND decided_at < @to
    GROUP BY 1
),
confirmed AS (
    SELECT lower(tags_confirmed_by) AS who, COUNT(*) AS n, MAX(tags_confirmed_at) AS last_at
    FROM frl.frl_movie_key_images
    WHERE tags_confirmed_by IS NOT NULL
      AND tags_confirmed_at >= @from AND tags_confirmed_at < @to
    GROUP BY 1
),
changes AS (
    SELECT lower(changed_by) AS who, COUNT(*) AS n, MAX(changed_at) AS last_at
    FROM frl.frl_movie_key_image_tag_changes
    WHERE changed_by IS NOT NULL AND changed_at >= @from AND changed_at < @to
    GROUP BY 1
),
movies AS (
    SELECT lower(tagger) AS who,
           COUNT(*) AS allocated,
           COUNT(*) FILTER (
               WHERE tags_confirmed_at >= @from AND tags_confirmed_at < @to) AS finished,
           MAX(tagger) AS name
    FROM frl.frl_movie_tagger_assignments
    GROUP BY 1
),
who AS (
    SELECT who FROM decided
    UNION SELECT who FROM confirmed
    UNION SELECT who FROM changes
    UNION SELECT who FROM movies
)
SELECT COALESCE(m.name, w.who),
       COALESCE(m.allocated, 0), COALESCE(m.finished, 0),
       COALESCE(d.kept, 0), COALESCE(d.discarded, 0),
       COALESCE(c.n, 0), COALESCE(ch.n, 0),
       GREATEST(COALESCE(d.last_at, 'epoch'::timestamptz),
                COALESCE(c.last_at, 'epoch'::timestamptz),
                COALESCE(ch.last_at, 'epoch'::timestamptz))
FROM who w
LEFT JOIN decided   d  ON d.who  = w.who
LEFT JOIN confirmed c  ON c.who  = w.who
LEFT JOIN changes   ch ON ch.who = w.who
LEFT JOIN movies    m  ON m.who  = w.who
WHERE w.who <> ''
ORDER BY COALESCE(c.n, 0) + COALESCE(d.kept, 0) DESC;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@from", from);
            cmd.Parameters.AddWithValue("@to", to);

            var rows = new List<TaggerStat>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var last = reader.GetDateTime(7);
                rows.Add(new TaggerStat(
                    reader.GetString(0),
                    reader.GetInt32(1), reader.GetInt32(2), reader.GetInt32(3),
                    reader.GetInt32(4), reader.GetInt32(5), reader.GetInt32(6),
                    last.Year <= 1970 ? null : last));
            }
            return rows;
        }

        /// <summary>
        /// Confirmed answers against changed ones per category: where the model
        /// is trusted, and where a tagger always has to correct it.
        /// </summary>
        public static async Task<List<CategoryStat>> CategoriesAsync(
            NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
        {
            const string sql = @"
WITH confirmed AS (
    SELECT t.category, COUNT(*) AS n
    FROM frl.frl_movie_key_image_tags t
    JOIN frl.frl_movie_key_images k ON k.id = t.key_image_id
    WHERE k.tags_confirmed_at >= @from AND k.tags_confirmed_at < @to
    GROUP BY 1
),
changed AS (
    SELECT category, COUNT(*) AS n
    FROM frl.frl_movie_key_image_tag_changes
    WHERE changed_at >= @from AND changed_at < @to
    GROUP BY 1
)
SELECT COALESCE(c.category, ch.category), COALESCE(c.n, 0), COALESCE(ch.n, 0)
FROM confirmed c
FULL OUTER JOIN changed ch ON ch.category = c.category
ORDER BY COALESCE(ch.n, 0) DESC;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@from", from);
            cmd.Parameters.AddWithValue("@to", to);

            var rows = new List<CategoryStat>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add(new CategoryStat(
                    reader.GetString(0), reader.GetInt32(1), reader.GetInt32(2)));
            return rows;
        }

        public static async Task<TaggingTotals> TotalsAsync(
            NpgsqlConnection connection, DateTime from, DateTime to, CancellationToken ct)
        {
            const string sql = @"
SELECT
    (SELECT COUNT(*) FROM frl.frl_movie_tagger_assignments
     WHERE assigned_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_tagger_assignments
     WHERE tags_confirmed_at >= @from AND tags_confirmed_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_key_images
     WHERE source = 'ai' AND created_at >= @from AND created_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_key_images
     WHERE decision = 'kept' AND decided_at >= @from AND decided_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_key_images
     WHERE decision = 'discarded' AND decided_at >= @from AND decided_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_key_images
     WHERE tagged_at >= @from AND tagged_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_key_images
     WHERE tags_confirmed_at >= @from AND tags_confirmed_at < @to),
    (SELECT COUNT(*) FROM frl.frl_movie_key_image_tag_changes
     WHERE changed_at >= @from AND changed_at < @to),
    (SELECT COUNT(DISTINCT lower(tagger)) FROM frl.frl_movie_tagger_assignments);";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@from", from);
            cmd.Parameters.AddWithValue("@to", to);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            await reader.ReadAsync(ct);
            return new TaggingTotals(
                (int)reader.GetInt64(0), (int)reader.GetInt64(1), (int)reader.GetInt64(2),
                (int)reader.GetInt64(3), (int)reader.GetInt64(4), (int)reader.GetInt64(5),
                (int)reader.GetInt64(6), (int)reader.GetInt64(7), (int)reader.GetInt64(8));
        }

        /// <summary>Where every allocated movie stands today, whatever the period.</summary>
        public static async Task<List<(string Stage, int Movies)>> StagesAsync(
            NpgsqlConnection connection, CancellationToken ct)
        {
            const string sql = @"
SELECT status, COUNT(*)
FROM frl.frl_movie_tagger_assignments
GROUP BY status;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            var rows = new List<(string, int)>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                rows.Add((reader.GetString(0), (int)reader.GetInt64(1)));
            return rows;
        }
    }
}
