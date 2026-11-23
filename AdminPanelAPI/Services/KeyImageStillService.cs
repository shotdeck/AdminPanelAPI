using Npgsql;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>A kept frame with no picture of its own in R2 yet.</summary>
    public sealed record KeyImageStillClaim(
        long Id, int MovieId, int? FrameNumber, double PositionSeconds);

    public interface IKeyImageStillService
    {
        /// <summary>
        /// Cut one frame out of the movie into R2 and hang it on the key
        /// image's row. Null if the movie has no video to cut from; anything
        /// the cutting service itself refuses is thrown.
        /// </summary>
        Task<string?> CutAsync(
            long id, int movieId, int? frameNumber, double positionSeconds, CancellationToken ct);
    }

    /// <summary>
    /// The picture a kept frame is shown and read on. It comes off the proxy,
    /// which is what the tagger judges terms on and is all a movie without a
    /// master has; the master's own frame is for publishing a still, later.
    ///
    /// Shared by the endpoints the tagging page calls and by the worker that
    /// cuts frames picked while watching, so a frame's picture is found the
    /// same way whoever asks for it.
    /// </summary>
    public sealed class KeyImageStillService : IKeyImageStillService
    {
        private readonly NpgsqlConnection _connection;
        private readonly IMovieFileStorageService _storage;
        private readonly IKeyImageAnalysisService _analysis;

        public KeyImageStillService(
            NpgsqlConnection connection,
            IMovieFileStorageService storage,
            IKeyImageAnalysisService analysis)
        {
            _connection = connection;
            _storage = storage;
            _analysis = analysis;
        }

        /// <summary>
        /// The frames a tagger picked by hand and which have no picture yet, as
        /// a proposal's frame already has the one its analysis cut. Oldest
        /// first, so a picking spree is caught up in the order it happened.
        /// </summary>
        public static async Task<List<KeyImageStillClaim>> PendingAsync(
            NpgsqlConnection connection, int limit, CancellationToken ct)
        {
            const string sql = @"
SELECT id, movie_id, frame_number, position_seconds
FROM frl.frl_movie_key_images
WHERE image_key IS NULL
  AND COALESCE(decision, 'kept') = 'kept'
ORDER BY id
LIMIT @limit;";

            var waiting = new List<KeyImageStillClaim>();
            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@limit", limit);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                waiting.Add(new KeyImageStillClaim(
                    reader.GetInt64(0),
                    reader.GetInt32(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2),
                    (double)reader.GetDecimal(3)));

            return waiting;
        }

        public async Task<string?> CutAsync(
            long id, int movieId, int? frameNumber, double positionSeconds, CancellationToken ct)
        {
            var variants = await _storage.GetMovieVariantsAsync(new[] { movieId }, ct);
            var variant = variants.FirstOrDefault();
            var sourceKey = variant?.SlimKey ?? variant?.HdKey;
            if (string.IsNullOrWhiteSpace(sourceKey))
                return null;

            // A frame number counts the master's frames, so it only means the
            // same thing on the file it was read off. Against the proxy it is
            // the position in seconds that holds.
            var fromProxy = variant?.SlimKey != null;
            var askFrame = fromProxy ? null : frameNumber;
            var ratios = await KeyImageCrop.RatiosAsync(_connection, movieId, ct);
            var result = await _analysis.CutStillAsync(
                sourceKey, askFrame, askFrame.HasValue ? null : positionSeconds, ratios, ct);
            if (!result.IsSuccess)
                throw new InvalidOperationException(result.Body);

            using var document = JsonDocument.Parse(result.Body);
            if (!document.RootElement.TryGetProperty("imageKey", out var cutKey) ||
                cutKey.GetString() is not { Length: > 0 } pictureKey)
                throw new InvalidOperationException("The still service returned no image.");

            // Only a cut off the master counts the frames the row means, so a
            // proxy's own frame number is not written over it.
            var cutFrame = !fromProxy &&
                document.RootElement.TryGetProperty("frame", out var frame) &&
                frame.TryGetInt32(out var number)
                ? number
                : frameNumber;

            // The crop is settled before the still is published, so the two
            // land together.
            var (crop, cropSource, croppedKey) =
                await ResolveCropAsync(id, pictureKey, document.RootElement, ct);

            const string saveSql = @"
UPDATE frl.frl_movie_key_images
SET image_key = @imageKey,
    frame_number = COALESCE(@frame, frame_number),
    crop_x = @x, crop_y = @y, crop_w = @w, crop_h = @h,
    crop_source = @source, cropped_key = @croppedKey
WHERE id = @id;";
            await using (var save = new NpgsqlCommand(saveSql, _connection))
            {
                save.Parameters.AddWithValue("@imageKey", pictureKey);
                save.Parameters.AddWithValue("@frame", (object?)cutFrame ?? DBNull.Value);
                KeyImageCrop.AddBox(save, crop);
                save.Parameters.Add(new NpgsqlParameter("@source", NpgsqlTypes.NpgsqlDbType.Varchar)
                { Value = (object?)cropSource ?? DBNull.Value });
                save.Parameters.AddWithValue("@croppedKey", (object?)croppedKey ?? DBNull.Value);
                save.Parameters.AddWithValue("@id", id);
                await save.ExecuteNonQueryAsync(ct);
            }

            // A frame picked while watching only gets a picture in R2 here, so
            // this is the first moment its terms can be read.
            await KeyImageTagStore.QueueOneAsync(_connection, id, ct);

            return pictureKey;
        }

        /// <summary>
        /// A crop a tagger chose is cut again on the new still; otherwise the
        /// still's own bar reading is taken.
        /// </summary>
        private async Task<(CropBox? Crop, string? Source, string? CroppedKey)> ResolveCropAsync(
            long id, string pictureKey, JsonElement cut, CancellationToken ct)
        {
            string? source = null;
            CropBox? chosen = null;
            const string sql = "SELECT " + KeyImageCrop.Columns +
                " FROM frl.frl_movie_key_images WHERE id = @id;";
            await using (var cmd = new NpgsqlCommand(sql, _connection))
            {
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                if (await reader.ReadAsync(ct))
                {
                    chosen = KeyImageCrop.Read(reader, 0);
                    source = reader.IsDBNull(4) ? null : reader.GetString(4);
                }
            }

            if (source == "tagger")
            {
                string? croppedKey = null;
                if (chosen != null)
                {
                    // A crop that fails to cut does not hold the still back: the
                    // frame is published with the tagger's crop kept and no cut
                    // copy, so applying the crop again in the editor finishes it.
                    var result = await _analysis.CropAsync(new[] { pictureKey }, chosen, ct);
                    if (result.IsSuccess)
                    {
                        using var doc = JsonDocument.Parse(result.Body);
                        if (doc.RootElement.TryGetProperty("cropped", out var map) &&
                            map.TryGetProperty(pictureKey, out var key))
                            croppedKey = key.GetString();
                    }
                }
                return (chosen, "tagger", croppedKey);
            }

            var auto = KeyImageCrop.Read(cut);
            var autoKey = auto != null && cut.TryGetProperty("croppedKey", out var cutKey)
                ? cutKey.GetString() : null;
            return (auto, auto == null ? null : "auto", autoKey);
        }
    }
}
