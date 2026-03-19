using FuzzySharp;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace ShotDeckSearch.Helpers
{
    public class SearchKeywordMatchResult
    {
        public List<string> Include { get; set; } = new();
        public List<string> Exclude { get; set; } = new();
    }

    public class KeywordParser
    {
        private static readonly Dictionary<string, bool?> phrases = new(StringComparer.OrdinalIgnoreCase)
        {
            ["but not including"] = false,
            ["but no"] = false,
            ["excluding"] = false,
            ["except"] = false,
            ["without"] = false,
            ["exclude"] = false,
            ["do not include"] = false,
            ["does not include"] = false,
            ["dont include"] = false,
            ["don't include"] = false,
            ["but don't include"] = false,
            ["not including"] = false,
            ["omit"] = false,
            ["nor"] = false,
            ["avoid"] = false,
            ["cut out"] = false,
            ["should not include"] = false,
            ["need to avoid"] = false,
            ["must not include"] = false,
            ["no need for"] = false,
            ["skip"] = false,
            ["not"] = false,
            ["leave out"] = false,
            ["dont show"] = false,
            ["don't show"] = false,

            ["add in"] = true,
            ["make sure to include"] = true,
            ["need to have"] = true,
            ["require"] = true,
            ["should have"] = true,
            ["must have"] = true,
            ["use"] = true,
            ["apply"] = true,
            ["but do include"] = true,
            ["including"] = true,
            ["include"] = true,
            ["with"] = true,
            ["must include"] = true,
            ["should include"] = true,
            ["only include"] = true,
            ["add"] = true,
            ["also include"] = true,

            ["and"] = null,
            ["or"] = null,
            [","] = null
        };

        public static SearchKeywordMatchResult ClassifyMatchedKeywords(
           string prompt,
           List<string> matchedKeywords)
        {
            var result = new SearchKeywordMatchResult();
            if (string.IsNullOrWhiteSpace(prompt) || matchedKeywords.Count == 0)
                return result;

            string lowered = prompt.ToLowerInvariant();
            int index = 0;
            bool currentMode = true;

            var segments = new List<(bool isInclude, string text)>();

            // Break the prompt into include/exclude segments
            while (index < lowered.Length)
            {
                var match = phrases
                    .Where(p => p.Value.HasValue)
                    .Select(p => new
                    {
                        Phrase = p.Key,
                        Value = p.Value.Value,
                        Index = lowered.IndexOf(p.Key, index, StringComparison.OrdinalIgnoreCase)
                    })
                    .Where(p => p.Index >= 0)
                    .OrderBy(p => p.Index)
                    .FirstOrDefault();

                if (match == null)
                {
                    segments.Add((currentMode, prompt.Substring(index).Trim()));
                    break;
                }

                if (match.Index > index)
                {
                    string segmentText = prompt.Substring(index, match.Index - index).Trim();
                    segments.Add((currentMode, segmentText));
                }

                currentMode = match.Value;
                index = match.Index + match.Phrase.Length;
            }

            // Match keywords against segments
            foreach (var keyword in matchedKeywords)
            {
                var keywordLower = keyword.ToLowerInvariant();

                foreach (var (isInclude, segment) in segments)
                {
                    if (segment.ToLowerInvariant().Contains(keywordLower))
                    {
                        var target = isInclude ? result.Include : result.Exclude;

                        if (!target.Contains(keyword, StringComparer.OrdinalIgnoreCase))
                            target.Add(keyword);

                        break; // only assign once
                    }
                }
            }

            return result;
        }
    }
}