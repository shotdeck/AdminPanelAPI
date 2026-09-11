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
        public static async Task<int> CountAsync(
            NpgsqlConnection connection, int movieId, CancellationToken ct)
        {
            const string sql = @"
SELECT count(*) FROM frl.frl_movie_key_images
WHERE movie_id = @movieId AND decision = 'proposed';";
            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            return (int)(long)(await cmd.ExecuteScalarAsync(ct) ?? 0L);
        }

        /// <summary>
        /// Put a finished job's proposals in the key image table as undecided
        /// frames, so the movie holds this run and only this run: a frame the
        /// run still proposes keeps whatever was decided about it and takes the
        /// new scores, and every other frame the analysis produced goes, so
        /// re-analysing does not pile run on run. Frames picked by hand while
        /// watching are not the analysis's to remove and survive untouched.
        /// </summary>
        public static async Task<int> StoreAsync(
            NpgsqlConnection connection, int movieId, JsonElement proposals, CancellationToken ct)
        {
            var positions = new List<decimal>();
            var frames = new List<int>();
            var scores = new List<decimal>();
            var looks = new List<decimal>();
            var stories = new List<decimal>();
            var keys = new List<string>();

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
            }

            if (positions.Count == 0)
                return 0;

            const string sql = @"
INSERT INTO frl.frl_movie_key_images
    (movie_id, position_seconds, frame_number, score, look_score, story_score,
     image_key, source, decision)
SELECT @movieId, position, frame, score, look, story, image_key, 'ai', 'proposed'
FROM unnest(@positions, @frames, @scores, @looks, @stories, @keys)
    AS proposal(position, frame, score, look, story, image_key)
ON CONFLICT (movie_id, position_seconds) DO UPDATE
    SET score       = EXCLUDED.score,
        look_score  = EXCLUDED.look_score,
        story_score = EXCLUDED.story_score,
        image_key   = EXCLUDED.image_key
    WHERE frl.frl_movie_key_images.decision = 'proposed'
      AND frl.frl_movie_key_images.source = 'ai';

DELETE FROM frl.frl_movie_key_images
WHERE movie_id = @movieId
  AND source = 'ai'
  AND position_seconds <> ALL(@positions);";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@movieId", movieId);
            cmd.Parameters.AddWithValue("@positions", positions.ToArray());
            cmd.Parameters.AddWithValue("@frames", frames.ToArray());
            cmd.Parameters.AddWithValue("@scores", scores.ToArray());
            cmd.Parameters.AddWithValue("@looks", looks.ToArray());
            cmd.Parameters.AddWithValue("@stories", stories.ToArray());
            cmd.Parameters.AddWithValue("@keys", keys.ToArray());
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
