using Npgsql;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>The picture inside a frame's bars, as fractions of the frame.</summary>
    public sealed record CropBox(double X, double Y, double W, double H)
    {
        public bool IsFull => X <= 0.0005 && Y <= 0.0005 && W >= 0.9995 && H >= 0.9995;

        public bool IsValid =>
            X >= 0 && Y >= 0 && W > 0.01 && H > 0.01 &&
            X + W <= 1.0005 && Y + H <= 1.0005;
    }

    /// <summary>
    /// A key image's crop. The uncropped still stays at image_key; the cut copy
    /// is at cropped_key, and is what the technical tags are read from.
    /// crop_source is "auto" when bar detection set it and "tagger" once
    /// somebody has, so a later re-cut of the still keeps a tagger's choice.
    /// crop_check marks an auto crop the detector had to guess (a frame too
    /// dark to read, or bars that matched none of the film's boxes); it stays
    /// until a tagger sets the crop.
    /// </summary>
    public static class KeyImageCrop
    {
        public const string Schema = @"
ALTER TABLE frl.frl_movie_key_images
    ADD COLUMN IF NOT EXISTS crop_x      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_y      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_w      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_h      NUMERIC(6,5),
    ADD COLUMN IF NOT EXISTS crop_source VARCHAR(16),
    ADD COLUMN IF NOT EXISTS cropped_key TEXT,
    ADD COLUMN IF NOT EXISTS cropped_by  VARCHAR(120),
    ADD COLUMN IF NOT EXISTS cropped_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS crop_check  BOOLEAN NOT NULL DEFAULT false;";

        /// <summary>The select list that <see cref="Json"/> reads, in order.</summary>
        public const string Columns = "crop_x, crop_y, crop_w, crop_h, crop_source, cropped_key, crop_check";

        public static CropBox? Read(JsonElement owner, string name = "crop")
        {
            if (!owner.TryGetProperty(name, out var crop) || crop.ValueKind != JsonValueKind.Object)
                return null;
            double Part(string part) =>
                crop.TryGetProperty(part, out var value) && value.TryGetDouble(out var number)
                    ? number : double.NaN;
            var box = new CropBox(Part("x"), Part("y"), Part("w"), Part("h"));
            return box.IsValid && !box.IsFull ? box : null;
        }

        public static CropBox? Read(NpgsqlDataReader reader, int first) =>
            reader.IsDBNull(first) ? null : new CropBox(
                (double)reader.GetDecimal(first), (double)reader.GetDecimal(first + 1),
                (double)reader.GetDecimal(first + 2), (double)reader.GetDecimal(first + 3));

        /// <summary>crop, cropSource, croppedKey and cropCheck for a row read with <see cref="Columns"/>.</summary>
        public static (object? Crop, string? Source, string? CroppedKey, bool Check) Json(
            NpgsqlDataReader reader, int first)
        {
            var box = Read(reader, first);
            return (
                box == null ? null : new { x = box.X, y = box.Y, w = box.W, h = box.H },
                reader.IsDBNull(first + 4) ? null : reader.GetString(first + 4),
                reader.IsDBNull(first + 5) ? null : reader.GetString(first + 5),
                !reader.IsDBNull(first + 6) && reader.GetBoolean(first + 6));
        }

        /// <summary>Whether a proposal says its auto crop is a guess.</summary>
        public static bool ReadCheck(JsonElement owner) =>
            owner.TryGetProperty("cropCheck", out var check) && check.ValueKind == JsonValueKind.True;

        public static async Task SaveAsync(
            NpgsqlConnection connection, long id, CropBox? crop, string? source,
            string? croppedKey, string? by, CancellationToken ct)
        {
            const string sql = @"
UPDATE frl.frl_movie_key_images
SET crop_x = @x, crop_y = @y, crop_w = @w, crop_h = @h,
    crop_source = @source, cropped_key = @croppedKey,
    cropped_by = COALESCE(@by, cropped_by),
    cropped_at = CASE WHEN @source = 'tagger' THEN now() ELSE cropped_at END,
    crop_check = CASE WHEN @source = 'tagger' THEN false ELSE crop_check END
WHERE id = @id;";
            await using var cmd = new NpgsqlCommand(sql, connection);
            AddBox(cmd, crop);
            cmd.Parameters.Add(new NpgsqlParameter("@source", NpgsqlTypes.NpgsqlDbType.Varchar)
            { Value = (object?)source ?? DBNull.Value });
            cmd.Parameters.AddWithValue("@croppedKey", (object?)croppedKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@by", (object?)by ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@id", id);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        public static void AddBox(NpgsqlCommand cmd, CropBox? crop)
        {
            object Part(double? value) =>
                value.HasValue ? Math.Round((decimal)value.Value, 5) : DBNull.Value;
            cmd.Parameters.Add(new NpgsqlParameter("@x", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = Part(crop?.X) });
            cmd.Parameters.Add(new NpgsqlParameter("@y", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = Part(crop?.Y) });
            cmd.Parameters.Add(new NpgsqlParameter("@w", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = Part(crop?.W) });
            cmd.Parameters.Add(new NpgsqlParameter("@h", NpgsqlTypes.NpgsqlDbType.Numeric) { Value = Part(crop?.H) });
        }

        /// <summary>
        /// The aspect ratios the movie's images are tagged with, e.g. "2.39,1.90",
        /// which auto crops must agree with. Read only; null if there are none.
        /// </summary>
        public static async Task<string?> RatiosAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct)
        {
            const string sql = @"
SELECT string_agg(DISTINCT aspect_ratio::text, ',')
FROM frl_images
WHERE movieid = @movieId AND aspect_ratio IS NOT NULL;";
            try
            {
                await using var cmd = new NpgsqlCommand(sql, connection);
                cmd.Parameters.AddWithValue("@movieId", movieId);
                return await cmd.ExecuteScalarAsync(ct) as string;
            }
            catch (PostgresException)
            {
                return null;
            }
        }
    }
}
