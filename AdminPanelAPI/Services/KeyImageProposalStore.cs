using Npgsql;
using System.Text.Json;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Where a finished analysis's proposals land. Shared by the tagger's own
    /// poll and by the worker that runs an analysis the moment a movie has an SF
    /// proxy, so both write the frames the same way.
    /// </summary>
    public static class KeyImageProposalStore
    {
        /// <summary>Proposals for one movie the tagger has yet to decide on.</summary>
        public static Task<int> CountAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct) =>
            CountAsync(connection, movieId, null, null, ct);

        /// <summary>
        /// The same count over one stretch of the film, for a shot read out of
        /// the movie's walkthrough.
        /// </summary>
        public static async Task<int> CountAsync(
            NpgsqlConnection connection,
            int movieId,
            double? fromSeconds,
            double? toSeconds,
            CancellationToken ct)
        {
            var sql = @"
SELECT count(*) FROM frl.frl_movie_key_images
WHERE movie_id = @movieId AND decision = 'proposed'" +
                (fromSeconds.HasValue ? " AND position_seconds >= @fromSeconds" : "") +
                (toSeconds.HasValue ? " AND position_seconds <= @toSeconds" : "") + ";";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            if (fromSeconds.HasValue)
                cmd.Parameters.AddWithValue("@fromSeconds", (decimal)fromSeconds.Value);
            if (toSeconds.HasValue)
                cmd.Parameters.AddWithValue("@toSeconds", (decimal)toSeconds.Value);
            return (int)(long)(await cmd.ExecuteScalarAsync(ct) ?? 0L);
        }

        /// <summary>
        /// Put a finished job's proposals in the key image table as undecided
        /// frames, so the movie holds this run and only this run of its kind: a
        /// frame the run still proposes keeps whatever was decided about it and
        /// takes the new scores, and every other frame judged the same way
        /// goes, so re-analysing does not pile run on run. A run judged the
        /// other way is left alone, which is what lets the walkthrough's
        /// frames and the plot's sit side by side for comparison; frames of
        /// unknown provenance, from before a run said, belong to neither and
        /// are cleared. Frames picked by hand while watching are not the
        /// analysis's to remove and survive untouched.
        /// </summary>
        public static async Task<int> StoreAsync(
            NpgsqlConnection connection,
            int movieId,
            JsonElement proposals,
            string? storyFrom,
            CancellationToken ct)
        {
            var positions = new List<decimal>();
            var frames = new List<int>();
            var scores = new List<decimal>();
            var looks = new List<decimal>();
            var stories = new List<decimal>();
            var keys = new List<string>();
            // -1 stands for "no crop"; Npgsql arrays of nullable numbers are awkward.
            var cropX = new List<double>();
            var cropY = new List<double>();
            var cropW = new List<double>();
            var cropH = new List<double>();
            var croppedKeys = new List<string?>();
            var cropChecks = new List<bool>();

            foreach (var proposal in proposals.EnumerateArray())
            {
                if (!proposal.TryGetProperty("imageKey", out var key) ||
                    key.GetString() is not { Length: > 0 } imageKey)
                    continue;

                positions.Add(Math.Round(proposal.GetProperty("seconds").GetDecimal(), 3));
                frames.Add(proposal.GetProperty("frame").GetInt32());
                scores.Add(proposal.TryGetProperty("score", out var score) ? score.GetDecimal() : 0m);
                looks.Add(Half(proposal, "look"));
                stories.Add(Half(proposal, "story"));
                keys.Add(imageKey);
                var crop = KeyImageCrop.Read(proposal);
                cropX.Add(crop?.X ?? -1);
                cropY.Add(crop?.Y ?? -1);
                cropW.Add(crop?.W ?? -1);
                cropH.Add(crop?.H ?? -1);
                croppedKeys.Add(crop != null &&
                    proposal.TryGetProperty("croppedKey", out var cut) &&
                    cut.GetString() is { Length: > 0 } cutKey ? cutKey : null);
                cropChecks.Add(KeyImageCrop.ReadCheck(proposal));
            }

            if (positions.Count == 0)
                return 0;

            const string sql = @"
INSERT INTO frl.frl_movie_key_images
    (movie_id, position_seconds, frame_number, score, look_score, story_score,
     story_from, image_key, source, decision,
     crop_x, crop_y, crop_w, crop_h, crop_source, cropped_key, crop_check)
SELECT @movieId, position, frame, score, look, story, @storyFrom, image_key,
       'ai', 'proposed',
       NULLIF(cx, -1), NULLIF(cy, -1), NULLIF(cw, -1), NULLIF(ch, -1),
       CASE WHEN cx = -1 THEN NULL ELSE 'auto' END, cropped_key, crop_check
FROM unnest(@positions, @frames, @scores, @looks, @stories, @keys,
            @cropX::numeric[], @cropY::numeric[], @cropW::numeric[], @cropH::numeric[],
            @croppedKeys::text[], @cropChecks::boolean[])
    AS proposal(position, frame, score, look, story, image_key, cx, cy, cw, ch, cropped_key,
                crop_check)
ON CONFLICT (movie_id, position_seconds, COALESCE(story_from, '')) DO UPDATE
    SET score       = EXCLUDED.score,
        look_score  = EXCLUDED.look_score,
        story_score = EXCLUDED.story_score,
        image_key   = EXCLUDED.image_key,
        crop_x      = EXCLUDED.crop_x,
        crop_y      = EXCLUDED.crop_y,
        crop_w      = EXCLUDED.crop_w,
        crop_h      = EXCLUDED.crop_h,
        crop_source = EXCLUDED.crop_source,
        cropped_key = EXCLUDED.cropped_key,
        crop_check  = EXCLUDED.crop_check
    WHERE frl.frl_movie_key_images.decision = 'proposed'
      AND frl.frl_movie_key_images.source = 'ai';

DELETE FROM frl.frl_movie_key_images
WHERE movie_id = @movieId
  AND source = 'ai'
  AND (story_from IS NULL OR story_from IS NOT DISTINCT FROM @storyFrom)
  AND (story_from IS DISTINCT FROM @storyFrom
       OR position_seconds <> ALL(@positions));";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@positions", positions.ToArray());
            cmd.Parameters.AddWithValue("@frames", frames.ToArray());
            cmd.Parameters.AddWithValue("@scores", scores.ToArray());
            cmd.Parameters.AddWithValue("@looks", looks.ToArray());
            cmd.Parameters.AddWithValue("@stories", stories.ToArray());
            cmd.Parameters.AddWithValue("@keys", keys.ToArray());
            cmd.Parameters.AddWithValue("@cropX", cropX.ToArray());
            cmd.Parameters.AddWithValue("@cropY", cropY.ToArray());
            cmd.Parameters.AddWithValue("@cropW", cropW.ToArray());
            cmd.Parameters.AddWithValue("@cropH", cropH.ToArray());
            cmd.Parameters.Add(new NpgsqlParameter("@croppedKeys",
                NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text)
            { Value = croppedKeys.Select(k => (object?)k ?? DBNull.Value).ToArray() });
            cmd.Parameters.AddWithValue("@cropChecks", cropChecks.ToArray());
            cmd.Parameters.AddWithValue(
                "@storyFrom", (object?)storyFrom ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(ct);

            return await CountAsync(connection, movieId, ct);
        }

        /// <summary>
        /// One half of a proposal's score, 0..1 within the run. Analyses from
        /// before the halves were reported have neither.
        /// </summary>
        private static decimal Half(JsonElement proposal, string name) =>
            proposal.TryGetProperty(name, out var value) &&
            value.ValueKind == JsonValueKind.Number
                ? Math.Round(value.GetDecimal(), 3)
                : 0m;
    }
}
