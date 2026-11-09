using System.Globalization;
using System.Text.Json;
using Npgsql;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>One image's metadata as the Qdrant importer lays it out: <c>{ image: {...}, movie: {...} }</c>.</summary>
public sealed class ImagePoint(long id, Dictionary<string, object?> payload, float[]? vector = null)
{
    public long Id { get; } = id;
    /// <summary>The 998-dim qwen3 vector; only loaded for inserts.</summary>
    public float[]? Vector { get; } = vector;
    public Dictionary<string, object?> Payload { get; } = payload;
    public Dictionary<string, object?> Image => (Dictionary<string, object?>)Payload["image"]!;
}

public sealed record FieldDifference(string Field, string Old, string New);

/// <summary>
/// Builds Qdrant payloads from PostgreSQL (read-only) and compares them with what's in Qdrant.
/// Field names and formats must stay exactly as the original importer wrote them, or every
/// point reads as changed.
/// </summary>
public static class QdrantPayloads
{
    public static readonly HashSet<string> ManagedImageKeys = new(StringComparer.Ordinal)
    {
        "movieid","status","search_excluded","time_period","aspect_ratio","int_ext","numpeople",
        "isNSFW","isViolent","isDrugs","weighted_score","date_added","clip_nsfw_violence",
        "width","height","date_updated","optical_format","format","actors","setting","setting_raw",
        "location","location_raw","filming_location","filming_location_raw","camera","lens","film_stock",
        "randid","shot_time","colors","time_of_day","gender","subject_age","subject_ethnicity",
        "frame_size","shot_types","movements","compositions","lens_types","lightings","lighting_types",
        "focus","clean_dirty","vfx_backing","location_type","lab_process","tags","rgb_colors"
    };

    public static readonly HashSet<string> ManagedMovieKeys = new(StringComparer.Ordinal)
    {
        "title","year","media_type","genre","mv_music_genre","director","cinematographer",
        "production_designer","costume_designer","editor","colorist","tv_showid","tv_season",
        "mv_artist","mv_featured_artist","mv_stylist","comm_brand","comm_product","trailer_description"
    };

    private static readonly string[] Sections = { "image", "movie" };

    private static HashSet<string> Managed(string section) => section == "image" ? ManagedImageKeys : ManagedMovieKeys;

    // ---------- PostgreSQL ----------

    /// <summary>Fails before any writes if a source column is missing or the TV columns aren't integers.</summary>
    public static async Task VerifySourceSchemaAsync(NpgsqlConnection db, CancellationToken ct, bool includeEmbeddings = false)
    {
        var expected = new Dictionary<string, string[]>(StringComparer.OrdinalIgnoreCase)
        {
            ["frl_images"] = new[] { "idnum", "movieid", "status", "search_excluded", "time_period", "aspect_ratio",
                "optical_format", "format", "numpeople", "int_ext", "isNSFW", "isViolent", "isDrugs",
                "weighted_score", "live_date", "clip_nsfw_violence", "width", "height", "updated", "actors", "setting", "location",
                "filming_location", "camera", "lens", "film_stock", "randid", "shot_time" },
            ["frl_movies"] = new[] { "idnum", "title", "year", "media_type", "genre", "mv_music_genre",
                "director", "cinematographer", "production_designer", "costume_designer", "editor", "colorist",
                "tv_showid", "tv_season", "mv_artist", "mv_featured_artist", "mv_stylist",
                "comm_brand", "comm_product", "trailer_description" },
            ["frl_join_images_tags"] = new[] { "imageid", "tag" }
        };
        if (includeEmbeddings)
            expected["frl_caption_embeddings_qwen3_mini_998"] = new[] { "idnum", "keyword_vectorized_embeddings" };
        await using var cmd = new NpgsqlCommand("""
            SELECT table_name, column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'frl' AND table_name = ANY(@tables)
            """, db);
        cmd.Parameters.AddWithValue("tables", expected.Keys.ToArray());
        var found = new Dictionary<string, Dictionary<string, string>>(StringComparer.OrdinalIgnoreCase);
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
            while (await reader.ReadAsync(ct))
            {
                var table = reader.GetString(0);
                if (!found.TryGetValue(table, out var cols)) found[table] = cols = new(StringComparer.OrdinalIgnoreCase);
                cols[reader.GetString(1)] = reader.GetString(2);
            }
        foreach (var (table, cols) in expected)
        {
            var present = found.GetValueOrDefault(table) ?? new(StringComparer.OrdinalIgnoreCase);
            var missing = cols.Where(c => !present.ContainsKey(c)).ToArray();
            if (missing.Length > 0)
                throw new InvalidOperationException($"Missing source columns in frl.{table}: {string.Join(", ", missing)}.");
        }
        foreach (var col in new[] { "tv_showid", "tv_season" })
        {
            var type = found["frl_movies"][col];
            if (type is not ("smallint" or "integer" or "bigint"))
                throw new InvalidOperationException($"frl_movies.{col} has type {type}, but Qdrant indexes it as integer.");
        }
    }

    /// <summary>The next <paramref name="limit"/> live image ids after <paramref name="afterId"/>.</summary>
    public static async Task<List<int>> ReadLiveImageIdsAsync(NpgsqlConnection db, int afterId, int limit, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT idnum FROM frl.frl_images WHERE idnum > @after AND status = 'live' ORDER BY idnum LIMIT @count", db);
        cmd.Parameters.AddWithValue("after", afterId);
        cmd.Parameters.AddWithValue("count", limit);
        var result = new List<int>(limit);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct)) result.Add(rdr.GetInt32(0));
        return result;
    }

    /// <summary>Image ids (any status) that have a 998-dim embedding, i.e. can be inserted into Qdrant.</summary>
    public static async Task<List<int>> ReadEmbeddedImageIdsAsync(NpgsqlConnection db, int afterId, int limit, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("""
            SELECT i.idnum
            FROM frl.frl_images i
            INNER JOIN frl.frl_caption_embeddings_qwen3_mini_998 e ON e.idnum = i.idnum
            WHERE i.idnum > @after AND e.keyword_vectorized_embeddings IS NOT NULL
            ORDER BY i.idnum
            LIMIT @count
            """, db);
        cmd.Parameters.AddWithValue("after", afterId);
        cmd.Parameters.AddWithValue("count", limit);
        var result = new List<int>(limit);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct)) result.Add(rdr.GetInt32(0));
        return result;
    }

    /// <summary>Complete managed payloads (no vectors) for the given images, join-table fields included.</summary>
    public static async Task<List<ImagePoint>> BuildPayloadsAsync(
        NpgsqlConnection db, int[] ids, bool includeColorShades, CancellationToken ct, bool withVectors = false)
    {
        var points = await ReadImagesAsync(db, ids, withVectors, ct);
        await FetchTagsAsync(db, points, ct);
        await FetchFreeTagsAsync(db, points, ct);
        if (includeColorShades) await FetchColorShadesAsync(db, points, ct);
        return points;
    }

    /// <summary>
    /// Images (and their movie) by id. With <paramref name="withVectors"/>, also the 998-dim
    /// vector from frl_caption_embeddings_qwen3_mini_998; images without one are skipped.
    /// </summary>
    private static async Task<List<ImagePoint>> ReadImagesAsync(NpgsqlConnection db, int[] ids, bool withVectors, CancellationToken ct)
    {
        var sql = """
            SELECT i.idnum, i.movieid, i.status::text, i.search_excluded,
                   i.time_period::text, i.aspect_ratio::text, i.optical_format::text,
                   i.format::text, i.numpeople::text, i.int_ext::text,
                   i."isNSFW", i."isViolent", i."isDrugs",
                   {VECTOR},
                   i.weighted_score, i.live_date::text, i.clip_nsfw_violence,
                   m.title::text, m.year, m.media_type::text, m.genre::text,
                   m.mv_music_genre::text, m.director::text, m.cinematographer::text,
                   m.production_designer::text, m.costume_designer::text,
                   i.actors::text, i.setting::text, i.location::text,
                   i.filming_location::text, i.camera::text, i.lens::text,
                   i.film_stock::text, i.randid::text, i.shot_time::text,
                   m.editor::text, m.colorist::text, m.tv_showid, m.tv_season,
                   m.mv_artist::text, m.mv_featured_artist::text, m.mv_stylist::text,
                   m.comm_brand::text, m.comm_product::text, m.trailer_description::text,
                   i.width, i.height, i.updated::text
            FROM frl.frl_images AS i
            {VECTOR_JOIN}
            LEFT JOIN frl.frl_movies AS m ON m.idnum = i.movieid
            WHERE i.idnum = ANY(@ids)
            ORDER BY i.idnum
            """
            .Replace("{VECTOR}", withVectors ? "e.keyword_vectorized_embeddings::text" : "NULL::text")
            .Replace("{VECTOR_JOIN}", withVectors
                ? "LEFT JOIN frl.frl_caption_embeddings_qwen3_mini_998 AS e ON e.idnum = i.idnum" : "");
        await using var cmd = new NpgsqlCommand(sql, db);
        cmd.Parameters.AddWithValue("ids", ids);
        var result = new List<ImagePoint>(ids.Length);
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
        {
            var id = Convert.ToInt64(rdr.GetValue(0), CultureInfo.InvariantCulture);
            float[]? vector = null;
            if (withVectors)
            {
                if (rdr.IsDBNull(13)) continue;
                try { vector = ParseVector(rdr.GetString(13)); }
                catch (FormatException e) { throw new InvalidDataException($"Bad vector for image {id}: {e.Message}", e); }
                if (vector.Any(x => !float.IsFinite(x)))
                    throw new InvalidDataException($"Image {id}: vector has non-finite values.");
            }
            var image = new Dictionary<string, object?>
            {
                ["movieid"] = DbLong(rdr, 1),
                ["status"] = DbString(rdr, 2),
                ["search_excluded"] = DbLong(rdr, 3),
                ["time_period"] = DbString(rdr, 4),
                ["aspect_ratio"] = DbString(rdr, 5),
                ["int_ext"] = DbString(rdr, 9),
                ["numpeople"] = DbString(rdr, 8),
                ["isNSFW"] = DbLong(rdr, 10),
                ["isViolent"] = DbLong(rdr, 11),
                ["isDrugs"] = DbLong(rdr, 12),
                ["weighted_score"] = DbLong(rdr, 14),
                ["date_added"] = IsoTimestamp(DbString(rdr, 15)),
                ["clip_nsfw_violence"] = DbLong(rdr, 16),
                ["width"] = DbLong(rdr, 45),
                ["height"] = DbLong(rdr, 46),
                ["date_updated"] = IsoTimestamp(DbString(rdr, 47))
            };
            PutCommaArray(image, "optical_format", DbString(rdr, 6));
            PutCommaArray(image, "format", DbString(rdr, 7));
            var movie = new Dictionary<string, object?>
            {
                ["title"] = DbString(rdr, 17),
                ["year"] = DbLong(rdr, 18),
                ["media_type"] = DbString(rdr, 19),
            };
            PutCommaArray(movie, "genre", DbString(rdr, 20));
            PutCommaArray(movie, "mv_music_genre", DbString(rdr, 21));
            PutCommaArray(movie, "director", DbString(rdr, 22));
            PutCommaArray(movie, "cinematographer", DbString(rdr, 23));
            PutCommaArray(movie, "production_designer", DbString(rdr, 24));
            PutCommaArray(movie, "costume_designer", DbString(rdr, 25));
            PutCommaArray(image, "actors", DbString(rdr, 26));
            PutHierarchy(image, "setting", DbString(rdr, 27));
            PutHierarchy(image, "location", DbString(rdr, 28));
            PutHierarchy(image, "filming_location", DbString(rdr, 29));
            PutSimple(image, "camera", DbString(rdr, 30));
            PutSimple(image, "lens", DbString(rdr, 31));
            PutSimple(image, "film_stock", DbString(rdr, 32));
            PutSimple(image, "randid", DbString(rdr, 33));
            PutSimple(image, "shot_time", DbString(rdr, 34));
            PutCommaArray(movie, "editor", DbString(rdr, 35));
            PutCommaArray(movie, "colorist", DbString(rdr, 36));
            movie["tv_showid"] = DbLong(rdr, 37);
            movie["tv_season"] = DbLong(rdr, 38);
            PutCommaArray(movie, "mv_artist", DbString(rdr, 39));
            PutCommaArray(movie, "mv_featured_artist", DbString(rdr, 40));
            PutCommaArray(movie, "mv_stylist", DbString(rdr, 41));
            PutCommaArray(movie, "comm_brand", DbString(rdr, 42));
            PutCommaArray(movie, "comm_product", DbString(rdr, 43));
            PutSimple(movie, "trailer_description", DbString(rdr, 44));
            result.Add(new ImagePoint(id, new Dictionary<string, object?> { ["image"] = image, ["movie"] = movie }, vector));
        }
        return result;
    }

    private static async Task FetchTagsAsync(NpgsqlConnection db, List<ImagePoint> points, CancellationToken ct)
    {
        if (points.Count == 0) return;
        const string sql = """
            SELECT imageid, field, value FROM (
                SELECT imageid, 'colors' AS field, color::text AS value FROM frl.frl_join_images_color WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'time_of_day', time_of_day::text FROM frl.frl_join_images_time_of_day WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'gender', gender::text FROM frl.frl_join_images_gender WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'subject_age', subject_age::text FROM frl.frl_join_images_subject_age WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'subject_ethnicity', subject_ethnicity::text FROM frl.frl_join_images_subject_ethnicity WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'frame_size', frame_size::text FROM frl.frl_join_images_frame_size WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'shot_types', shot_type::text FROM frl.frl_join_images_shot_type WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'movements', camera_movements::text FROM frl.frl_join_images_camera_movements WHERE imageid = ANY(@ids) AND status = 'ok'
                UNION ALL SELECT imageid, 'compositions', composition::text FROM frl.frl_join_images_composition WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'lens_types', lens_type::text FROM frl.frl_join_images_lens_type WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'lightings', lighting::text FROM frl.frl_join_images_lighting WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'lighting_types', lighting_type::text FROM frl.frl_join_images_lighting_type WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'focus', focus::text FROM frl.frl_join_images_focus WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'clean_dirty', clean_dirty::text FROM frl.frl_join_images_clean_dirty WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'vfx_backing', vfx_backing::text FROM frl.frl_join_images_vfx_backing WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'location_type', location_type::text FROM frl.frl_join_images_location_type WHERE imageid = ANY(@ids)
                UNION ALL SELECT imageid, 'lab_process', lab_process::text FROM frl.frl_join_images_lab_process WHERE imageid = ANY(@ids)
            ) src
            WHERE value IS NOT NULL AND value <> ''
            """;
        var byId = points.ToDictionary(p => checked((int)p.Id));
        var tags = new Dictionary<(int Id, string Field), HashSet<string>>();
        await using var cmd = new NpgsqlCommand(sql, db);
        cmd.Parameters.AddWithValue("ids", byId.Keys.ToArray());
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
        {
            var key = (rdr.GetInt32(0), rdr.GetString(1));
            if (!tags.TryGetValue(key, out var set)) tags[key] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            set.Add(rdr.GetString(2).Trim());
        }
        foreach (var kv in tags)
            if (byId.TryGetValue(kv.Key.Id, out var point))
                point.Image[kv.Key.Field] = kv.Value.OrderBy(s => s, StringComparer.OrdinalIgnoreCase).ToArray();
    }

    private static async Task FetchFreeTagsAsync(NpgsqlConnection db, List<ImagePoint> points, CancellationToken ct)
    {
        if (points.Count == 0) return;
        var byId = points.ToDictionary(p => checked((int)p.Id));
        var tags = new Dictionary<int, HashSet<string>>();
        await using var cmd = new NpgsqlCommand("""
            SELECT imageid, tag::text FROM frl.frl_join_images_tags
            WHERE imageid = ANY(@ids) AND tag IS NOT NULL
            """, db);
        cmd.Parameters.AddWithValue("ids", byId.Keys.ToArray());
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
        {
            var value = rdr.GetString(1).Trim();
            if (value.Length == 0) continue;
            var id = rdr.GetInt32(0);
            if (!tags.TryGetValue(id, out var set)) tags[id] = set = new(StringComparer.OrdinalIgnoreCase);
            set.Add(value);
        }
        foreach (var (id, values) in tags)
            byId[id].Image["tags"] = values.OrderBy(x => x).ToArray();
    }

    private static async Task FetchColorShadesAsync(NpgsqlConnection db, List<ImagePoint> points, CancellationToken ct)
    {
        if (points.Count == 0) return;
        var byId = points.ToDictionary(p => checked((int)p.Id));
        var grouped = new Dictionary<int, List<JsonElement>>();
        await using var cmd = new NpgsqlCommand("""
            SELECT imageid, to_jsonb(c)::text
            FROM frl.frl_join_images_color_shades AS c
            WHERE imageid = ANY(@ids)
            """, db);
        cmd.Parameters.AddWithValue("ids", byId.Keys.ToArray());
        await using var rdr = await cmd.ExecuteReaderAsync(ct);
        while (await rdr.ReadAsync(ct))
        {
            var id = rdr.GetInt32(0);
            using var doc = JsonDocument.Parse(rdr.GetString(1));
            if (!grouped.TryGetValue(id, out var items)) grouped[id] = items = new List<JsonElement>();
            items.Add(doc.RootElement.Clone());
        }
        foreach (var kv in grouped)
            if (byId.TryGetValue(kv.Key, out var point)) point.Image["rgb_colors"] = kv.Value;
    }

    // ---------- Comparison ----------

    /// <summary>
    /// The payload to write back, or null when Qdrant already matches. Only managed keys are
    /// replaced; anything else already on the point is kept. A managed key that is no longer
    /// set in PostgreSQL is removed.
    /// </summary>
    public static Dictionary<string, object?>? Reconcile(
        Dictionary<string, object?> existing, Dictionary<string, object?> source, bool includeColorShades)
    {
        var oldRoot = JsonSerializer.Deserialize<Dictionary<string, object?>>(JsonSerializer.Serialize(existing))!;
        var updated = new Dictionary<string, object?>(oldRoot, StringComparer.Ordinal);
        var changed = false;
        foreach (var section in Sections)
        {
            var oldSection = ToDictionary(oldRoot.TryGetValue(section, out var o) ? o : null);
            var sourceSection = ToDictionary(source[section]);
            var resultSection = new Dictionary<string, object?>(oldSection, StringComparer.Ordinal);
            foreach (var key in Managed(section))
            {
                if (key == "rgb_colors" && !includeColorShades) continue;
                if (sourceSection.TryGetValue(key, out var v)) resultSection[key] = v;
                else resultSection.Remove(key);
            }
            if (!Equivalent(oldSection, resultSection)) { updated[section] = resultSection; changed = true; }
        }
        return changed ? updated : null;
    }

    /// <summary>Per-field differences over the same managed keys as <see cref="Reconcile"/>; a missing key differs from an explicit null.</summary>
    public static List<FieldDifference> Differences(
        Dictionary<string, object?> existing, Dictionary<string, object?> source, bool includeColorShades)
    {
        var output = new List<FieldDifference>();
        foreach (var section in Sections)
        {
            var oldPart = ToDictionary(existing.TryGetValue(section, out var oldValue) ? oldValue : null);
            var newPart = ToDictionary(source[section]);
            foreach (var key in Managed(section).OrderBy(x => x, StringComparer.Ordinal))
            {
                if (key == "rgb_colors" && !includeColorShades) continue;
                var hadOld = oldPart.TryGetValue(key, out var oldField);
                var hasNew = newPart.TryGetValue(key, out var newField);
                if (hadOld != hasNew || (hadOld && !Equivalent(oldField, newField)))
                    output.Add(new FieldDifference($"{section}.{key}",
                        hadOld ? Brief(oldField) : "<missing>", hasNew ? Brief(newField) : "<missing>"));
            }
        }
        return output;
    }

    private static string Brief(object? value)
    {
        if (value is null) return "null";
        var json = JsonSerializer.Serialize(value);
        return json.Length <= 400 ? json : json[..400] + "... [truncated]";
    }

    private static Dictionary<string, object?> ToDictionary(object? value)
    {
        if (value is Dictionary<string, object?> dict) return new(dict, StringComparer.Ordinal);
        if (value is JsonElement e && e.ValueKind == JsonValueKind.Object)
            return JsonSerializer.Deserialize<Dictionary<string, object?>>(e.GetRawText())!;
        return new();
    }

    public static bool Equivalent(object? a, object? b) =>
        JsonEqual(JsonSerializer.SerializeToElement(a), JsonSerializer.SerializeToElement(b));

    private static bool JsonEqual(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind) return false;
        switch (a.ValueKind)
        {
            case JsonValueKind.Object:
                var ap = a.EnumerateObject().ToDictionary(x => x.Name, x => x.Value, StringComparer.Ordinal);
                var bp = b.EnumerateObject().ToDictionary(x => x.Name, x => x.Value, StringComparer.Ordinal);
                return ap.Count == bp.Count && ap.All(kv => bp.TryGetValue(kv.Key, out var v) && JsonEqual(kv.Value, v));
            case JsonValueKind.Array:
                var aa = a.EnumerateArray().ToArray();
                var ba = b.EnumerateArray().ToArray();
                return aa.Length == ba.Length && !aa.Where((v, i) => !JsonEqual(v, ba[i])).Any();
            case JsonValueKind.Number:
                return a.TryGetDecimal(out var x) && b.TryGetDecimal(out var y) ? x == y : a.GetRawText() == b.GetRawText();
            default:
                return a.GetRawText() == b.GetRawText();
        }
    }

    // ---------- Field formatting (matches the original importer) ----------

    /// <summary>A PostgreSQL date/timestamp (no time zone) as UTC round-trip ISO, e.g. 2014-10-21T00:00:00.0000000+00:00.</summary>
    public static string? IsoTimestamp(string? source)
    {
        if (string.IsNullOrWhiteSpace(source)) return null;
        if (DateTimeOffset.TryParse(source, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
            return parsed.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        throw new InvalidDataException($"Unrecognized image timestamp: {source}");
    }

    private static long? DbLong(NpgsqlDataReader reader, int i) =>
        reader.IsDBNull(i) ? null : Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);

    private static string? DbString(NpgsqlDataReader reader, int i) => reader.IsDBNull(i) ? null : reader.GetString(i);

    public const int VectorSize = 998;

    /// <summary>A pgvector text literal, e.g. [0.1,-2,3e-4], which must have exactly <see cref="VectorSize"/> values.</summary>
    public static float[] ParseVector(string input)
    {
        var text = input.AsSpan().Trim();
        if (text.Length < 2 || text[0] != '[' || text[^1] != ']') throw new FormatException("Expected pgvector array literal.");
        text = text[1..^1];
        var values = new float[VectorSize];
        var count = 0;
        while (!text.IsEmpty)
        {
            var comma = text.IndexOf(',');
            var token = comma < 0 ? text : text[..comma];
            if (count == values.Length) throw new FormatException("Too many elements.");
            values[count++] = float.Parse(token, NumberStyles.Float, CultureInfo.InvariantCulture);
            text = comma < 0 ? ReadOnlySpan<char>.Empty : text[(comma + 1)..];
        }
        if (count != values.Length) throw new FormatException($"Expected {VectorSize} elements, got {count}.");
        return values;
    }

    public static void PutCommaArray(Dictionary<string, object?> payload, string field, string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv)) return;
        payload[field] = csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    public static void PutSimple(Dictionary<string, object?> payload, string field, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value)) payload[field] = value.Trim();
    }

    /// <summary>Exact source text as <c>{field}_raw</c>, plus the colon-separated terms for filtering.</summary>
    public static void PutHierarchy(Dictionary<string, object?> payload, string field, string? text)
    {
        if (string.IsNullOrWhiteSpace(text)) return;
        payload[field + "_raw"] = text;
        payload[field] = text.Split(':', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }
}
