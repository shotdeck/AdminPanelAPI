using System.Text;
using System.Text.RegularExpressions;

namespace AdminPanelAPI.Helpers
{
    public static class KeywordMetadataBuilder
    {
        public static string BuildMetadata(
            Models.ImageRecord rec,
            Dictionary<int, List<string>> gender,
            Dictionary<int, List<string>> subjectAge,
            Dictionary<int, List<string>> subjectEth,
            Dictionary<int, List<string>> frameSize,
            Dictionary<int, List<string>> tags,
            Dictionary<int, List<string>> timeOfDay,
            Dictionary<int, (string? Title, string? Year, string? Genre, string? Product, string? Brand, string? MediaType, string? Director, string? Cinematographer)> movieFields)
        {
            var sb = new StringBuilder();

            if (rec.MovieId.HasValue && movieFields.TryGetValue(rec.MovieId.Value, out var mv))
            {
                string mediaLabel = GetMediaFieldName(mv.MediaType);
                AppendLineIfAny(sb, mediaLabel, mv.Title);
                AppendLineIfAny(sb, "Year", mv.Year);
                AppendLineIfAny(sb, "Genre", NormalizeGenres(mv.Genre));
                AppendLineIfAny(sb, "Product", mv.Product);
                AppendLineIfAny(sb, "Brand", mv.Brand);
                AppendLineIfAny(sb, "Director Directed by", mv.Director);
                AppendLineIfAny(sb, "Cinematographer Cinematography Filmed by", mv.Cinematographer);
            }

            AppendLineIfAny(sb, "Shot on", NormalizeFormat(Clean(rec.Format)));
            AppendLineIfAny(sb, "Optical format", NormalizeOpticalFormat(Clean(rec.OpticalFormat)));
            AppendLineIfAny(sb, "Time period", Clean(rec.TimePeriod));
            AppendLineIfAny(sb, "Setting", SplitColons(rec.Setting));
            AppendLineIfAny(sb, "Actors", NormalizeFormat(Clean(rec.Actors)));

            AppendLineIfAny(sb, null, NormalizeIntExtFormat(Clean(rec.IntExt)));

            var (locLine, filmLine) = BuildLocationFields(rec.Location, rec.FilmingLocation);
            if (!string.IsNullOrWhiteSpace(locLine)) sb.AppendLine(locLine);
            if (!string.IsNullOrWhiteSpace(filmLine)) sb.AppendLine(filmLine);

            AppendLineIfAny(sb, "Gender", GetPipeJoined(gender, rec.Id, null, toLower: true));
            AppendLineIfAny(sb, "Subject_age", GetPipeJoined(subjectAge, rec.Id, null, toLower: true));
            AppendLineIfAny(sb, "Subject ethnicity", GetPipeJoined(subjectEth, rec.Id, null, toLower: true));
            AppendLineIfAny(sb, "Shot size", GetPipeJoined(frameSize, rec.Id, NormalizeFrameSize, toLower: true));
            AppendLineIfAny(sb, "Tags", GetPipeJoined(tags, rec.Id, null, toLower: true));
            AppendLineIfAny(sb, "Time of Day", GetPipeJoined(timeOfDay, rec.Id, null, toLower: true));

            return NormalizeBlock(sb.ToString());
        }

        public static string GetMediaFieldName(string? mediaType)
        {
            var mt = Clean(mediaType);
            if (string.IsNullOrWhiteSpace(mt)) return "Movie";

            if (string.Equals(mt, "tv", StringComparison.OrdinalIgnoreCase))
                mt = "tv series";

            return char.ToUpperInvariant(mt[0]) + mt.Substring(1);
        }

        public static void AppendLineIfAny(StringBuilder sb, string? field, string? value)
        {
            if (string.IsNullOrWhiteSpace(field))
            {
                if (!string.IsNullOrWhiteSpace(value))
                    sb.AppendLine($"{value}");
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(value))
                    sb.AppendLine($"{field}: {value}");
            }
        }

        public static string NormalizeBlock(string s)
        {
            if (string.IsNullOrWhiteSpace(s)) return s;
            s = Regex.Replace(s, @"[ \t]+(\r?\n)", "$1");
            s = Regex.Replace(s, @"(\r?\n){2,}", "\n");
            s = s.Trim();
            return s;
        }

        public static string? GetPipeJoined(
            Dictionary<int, List<string>> source,
            int id,
            Func<string?, string?>? map = null,
            bool toLower = false)
        {
            if (!source.TryGetValue(id, out var values) || values == null || values.Count == 0)
                return null;

            IEnumerable<string?> cleaned = values.Select(v => Clean(v));
            if (map != null) cleaned = cleaned.Select(map);

            var uniq = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var outList = new List<string>();
            foreach (var v in cleaned)
            {
                if (string.IsNullOrWhiteSpace(v)) continue;
                var item = toLower ? v!.ToLowerInvariant() : v!;
                if (uniq.Add(item)) outList.Add(item);
            }
            return outList.Count > 0 ? string.Join(" | ", outList) : null;
        }

        public static string? SplitColons(string? input)
        {
            if (string.IsNullOrWhiteSpace(input)) return null;

            var tokens = input
                .Split(':', StringSplitOptions.RemoveEmptyEntries)
                .Select(t => t.Trim())
                .Where(t => t.Length > 0 && !t.Equals("--") && !Regex.IsMatch(t, "^:+$"));

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = new List<string>();
            foreach (var t in tokens)
                if (seen.Add(t)) list.Add(t);

            return list.Count > 0 ? string.Join(" | ", list) : null;
        }

        public static (string? LocationLine, string? FilmingLocationLine) BuildLocationFields(string? loc, string? filmLoc)
        {
            var setIn = TokenizeLocation(loc);
            var filmedIn = TokenizeLocation(filmLoc);

            if (setIn.Count == 0 && filmedIn.Count == 0) return (null, null);

            if (SetEqual(setIn, filmedIn) || IsSubset(setIn, filmedIn) || IsSubset(filmedIn, setIn))
            {
                var merged = setIn.Count >= filmedIn.Count ? setIn : filmedIn;
                return (JoinTokens("Filmed in", merged), null);
            }

            return (JoinTokens("Set in", setIn), JoinTokens("Filmed in", filmedIn));
        }

        public static List<string> TokenizeLocation(string? input)
        {
            if (string.IsNullOrWhiteSpace(input)) return new();

            var canon = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["United States of America"] = "United States",
                ["USA"] = "United States",
                ["U.S.A."] = "United States",
                ["U.S."] = "United States",
                ["Earth"] = ""
            };

            var tokens = input
                .Split(new[] { ':', '|', ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(t => t.Trim())
                .Where(t => t.Length > 0)
                .Select(t => canon.TryGetValue(t, out var m) ? (m ?? "") : t)
                .Select(t => t.Trim())
                .Where(t => t.Length > 0);

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = new List<string>();
            foreach (var t in tokens) if (seen.Add(t)) list.Add(t);
            return list;
        }

        public static string? JoinTokens(string field, List<string> toks) =>
            toks.Count == 0 ? null : $"{field}: {string.Join(" | ", toks)}";

        public static bool SetEqual(List<string> a, List<string> b) =>
            a.Count == b.Count && new HashSet<string>(a, StringComparer.OrdinalIgnoreCase).SetEquals(b);

        public static bool IsSubset(List<string> small, List<string> big) =>
            small.Count > 0 && new HashSet<string>(big, StringComparer.OrdinalIgnoreCase).IsSupersetOf(small);

        public static string? NormalizeGenres(string? genre)
        {
            if (string.IsNullOrWhiteSpace(genre)) return null;
            var parts = genre
                .Split(new[] { ',', '|' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(s => Regex.Replace(s.Trim(), @"\s+", " "))
                .Where(s => s.Length > 0);

            var uniq = new LinkedList<string>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in parts)
                if (seen.Add(p)) uniq.AddLast(p);

            return uniq.Count > 0 ? string.Join(" | ", uniq) : null;
        }

        public static string? NormalizeFormat(string? input)
        {
            input = Clean(input);
            if (input == null) return null;
            input = Regex.Replace(input, @"\b[Ff]ilm\s*-\s*", "");
            input = Regex.Replace(input, @"\s+", " ").Trim();
            return input.Length == 0 ? null : input;
        }

        public static string? NormalizeIntExtFormat(string? input)
        {
            input = Clean(input);
            if (input == null) return null;
            return input.ToLowerInvariant() switch
            {
                "interior" => "This is an interior shot",
                "exterior" => "This is an exterior shot",
                _ => null
            };
        }

        public static string? NormalizeOpticalFormat(string? input)
        {
            input = Clean(input);
            if (input == null) return null;

            var raw = input.Split(new[] { ',', '|', '/' }, StringSplitOptions.RemoveEmptyEntries)
                           .Select(s => s.Trim())
                           .Where(s => s.Length > 0);

            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["3 perf"] = "3-perf",
                ["4 perf"] = "4-perf",
                ["super35"] = "Super 35",
                ["super 35"] = "Super 35",
                ["s35"] = "Super 35",
            };

            string Fix(string s)
            {
                var key = Regex.Replace(s, @"\s+", " ").Trim();
                if (map.TryGetValue(key, out var norm)) return norm;
                return key;
            }

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = new List<string>();
            foreach (var item in raw.Select(Fix))
                if (seen.Add(item)) list.Add(item);

            return list.Count > 0 ? string.Join(" | ", list) : null;
        }

        public static string? NormalizeFrameSize(string? s)
        {
            s = Clean(s);
            if (s == null) return null;
            s = Regex.Replace(s, @"\s+", " ").Trim();

            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["medium close up"] = "medium close-up",
                ["mcu"] = "medium close-up",
                ["close up"] = "close-up",
                ["cu"] = "close-up",
                ["extreme close up"] = "extreme close-up",
                ["ecu"] = "extreme close-up",
                ["ws"] = "wide",
                ["ls"] = "wide",
            };

            return map.TryGetValue(s, out var norm) ? norm : s;
        }

        public static string? Clean(string? val)
        {
            if (string.IsNullOrWhiteSpace(val)) return null;
            val = Regex.Replace(val, @"\s+", " ").Trim();
            return (val == "--" || val.Equals("null", StringComparison.OrdinalIgnoreCase) || Regex.IsMatch(val, "^:+$"))
                ? null
                : val;
        }
    }
}
