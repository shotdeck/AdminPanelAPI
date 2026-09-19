using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// What an analysis is told the film is: frames are scored partly on how
    /// well they match this text. Shared by the tagger's prompt box and by the
    /// worker that analyses a movie before anyone asks.
    /// </summary>
    public static class MovieDescriptions
    {
        /// <summary>
        /// What the analysis looks for when a film has no synopsis stored: the
        /// text a frame is matched against, phrased as the picture it should be
        /// rather than as an instruction, since scoring is image/text similarity.
        /// </summary>
        public const string FrameCriteria =
            "A cinematic, well-composed film still: sharp focus, strong " +
            "lighting and colour, faces lit and eyes open, a moment that " +
            "looks emblematic of the film.";

        /// <summary>Marks prose that came from Wikipedia rather than a column.</summary>
        public const string WikipediaSource = "wikipedia";

        /// <summary>
        /// Name patterns for a column holding prose about the film. Guessing
        /// exact names missed the one frl_movies actually uses, so every text
        /// column whose name reads like a description is a candidate and the
        /// longest value a film has among them is used.
        /// </summary>
        private static readonly string[] DescriptionPatterns =
        {
            "%synops%", "%overview%", "%plot%", "%descript%", "%logline%",
            "%summar%", "%story%", "%blurb%", "%tagline%", "%abstract%"
        };

        private static string[]? _descriptionColumns;

        /// <summary>The prose-ish text columns frl_movies actually has.</summary>
        public static async Task<string[]> ColumnsAsync(
            NpgsqlConnection connection, CancellationToken ct)
        {
            if (_descriptionColumns != null)
                return _descriptionColumns;

            const string sql = @"
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'frl' AND table_name = 'frl_movies'
  AND data_type IN ('text', 'character varying', 'character')
  AND column_name ILIKE ANY(@patterns)
ORDER BY column_name;";

            await using var cmd = new NpgsqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@patterns", DescriptionPatterns);

            var found = new List<string>();
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                    found.Add(reader.GetString(0));
            }

            _descriptionColumns = found.ToArray();
            return _descriptionColumns;
        }

        /// <summary>
        /// What the film is, for scoring how relevant a frame is to it, plus
        /// where the prose came from so a missing synopsis can be told apart
        /// from a column this never found. frl_movies holds no synopsis today,
        /// so a film's plot is looked up on Wikipedia; failing that, the text
        /// falls back to the picture the analysis wants of any film.
        /// </summary>
        public static async Task<(string? Description, string? Source)> ForAsync(
            NpgsqlConnection connection,
            IFilmSynopsisService synopsis,
            int movieId,
            CancellationToken ct)
        {
            var candidates = await ColumnsAsync(connection, ct);
            var columns = string.Concat(candidates.Select(c => $", \"{c}\""));
            var sql = $"SELECT title, year{columns} FROM frl.frl_movies " +
                "WHERE idnum = @movieId LIMIT 1;";

            string name;
            int? year = null;
            var best = "";
            string? from = null;

            await using (var cmd = new NpgsqlCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@movieId", movieId);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct) || reader.IsDBNull(0))
                    return (null, null);

                name = reader.GetString(0).Trim();
                if (!reader.IsDBNull(1) &&
                    int.TryParse(reader.GetValue(1)?.ToString(), out var stored))
                    year = stored;

                for (var i = 0; i < candidates.Length; i++)
                {
                    if (reader.IsDBNull(i + 2)) continue;
                    var value = reader.GetString(i + 2).Trim();
                    if (value.Length <= best.Length) continue;
                    best = value;
                    from = candidates[i];
                }
            }

            var title = year is null ? name : $"{name} ({year})";
            if (best.Length > 0)
                return ($"{title}. {best}", from);

            var plot = await synopsis.LookupAsync(name, year, ct);
            return plot is { Length: > 0 }
                ? ($"{title}. {plot}", WikipediaSource)
                : ($"{title}. {FrameCriteria}", null);
        }
    }
}
