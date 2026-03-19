using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using ShotDeckSearch.Helpers;
using ShotDeckSearch.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
namespace ShotDeck.Keywords
{
    public interface IKeywordCacheService
    {
        IReadOnlyDictionary<string,
        List<string>> GetKeywordsByCategory();
        HashSet<string> GetFlatKeywordSet();
        IReadOnlyDictionary<string,
        IReadOnlyCollection<string>> GetKeywordSources();
        IReadOnlyCollection<string> GetSourcesFor(string keyword);
        IReadOnlyCollection<string> GetSynonymsForMaster(string master);
        List<string> Search(string text);
        List<SearchHit> SearchWithSources(string text);
        // NEW: for classification (must include raw hits like "zolly") 
        List<string> SearchForClassification(string text);
        // NEW: map synonym -> master for output & lookups 
        string Canonicalize(string term);
        KeywordCacheStatus GetKeywordCacheStatus();
        Task RefreshAsync();
        // Unwanted words methods - Dictionary maps phrase to is_super_blacklist
        IReadOnlyDictionary<string, bool> GetUnwantedWords();
        bool IsUnwantedWord(string word);
        bool IsSuperBlacklistMatch(string text);
        CensorshipCheckResult CheckCensorship(string text);
        IReadOnlyCollection<string> GetAllSynonymMasters();
        string? GetSynonymCategory(string keyword);
        IReadOnlyDictionary<string, HashSet<string>> GetKeywordToCategories();
        IReadOnlyCollection<string> GetImageTags();
        List<TagWithOrigin> SearchAllWithOrigin(string query);
    }
    public sealed class TagWithOrigin
    {
        public string Tag { get; init; } = default!;
        public string Origin { get; init; } = default!;
    }
    public sealed class CensorshipCheckResult
    {
        public bool RejectedForCensorship { get; init; }
        public bool SuperBlackList { get; init; }
        public string? MatchedWord { get; init; }
        public string? OriginalPhrase { get; init; }
    }
    public sealed record SearchHit(string Keyword, IReadOnlyCollection<string> Sources);
    public sealed class KeywordCacheStatus
    {
        public DateTimeOffset? LastCsvWarmAt
        {
            get;
            init;
        }
        public DateTimeOffset? LastRefreshStartAt
        {
            get;
            init;
        }
        public DateTimeOffset? LastRefreshEndAt
        {
            get;
            init;
        }
        public bool IsRefreshRunning
        {
            get;
            init;
        }
        public bool LastRefreshSucceeded
        {
            get;
            init;
        }
        public string? LastRefreshError
        {
            get;
            init;
        }
        public int FlatCount
        {
            get;
            init;
        }
        public int SourceKeys
        {
            get;
            init;
        }
        public int CategoryCount
        {
            get;
            init;
        }
        public int SynonymCount
        {
            get;
            init;
        }
        public int UnwantedWordsCount
        {
            get;
            init;
        }
        public int SuperBlacklistCount
        {
            get;
            init;
        }
    }
    internal sealed class KeywordSnapshot
    {
        public readonly HashSet<string> FlatSet;
        public readonly Dictionary<string,
        HashSet<string>> KeywordSources;
        public readonly Dictionary<string,
        List<string>> KeywordsByCategory;
        public readonly PhraseMatcher Matcher;
        public readonly Dictionary<string,
        string> SynonymToMaster;
        public readonly Dictionary<string, bool> UnwantedWords;
        public readonly HashSet<string> AllMasterTerms;
        public readonly Dictionary<string, string> MasterTermCategories;
        public readonly Dictionary<string, List<string>> MasterToSynonyms;
        public readonly Dictionary<string, HashSet<string>> KeywordToCategories;
        public readonly Dictionary<string, string> NormalizedUnwantedWords;
        public readonly HashSet<string> ImageTags;
        internal static readonly string FlatFile = "keywords_flat.csv";
        internal static readonly string SourcesFile = "keyword_sources.csv";
        internal static readonly string ByCategoryFile = "keywords_by_category.csv";
        internal static readonly string SynonymsFile = "keyword_synonyms.csv";
        internal static readonly string UnwantedWordsFile = "unwanted_words.csv";
        internal static readonly string MasterTermsFile = "master_terms.csv";
        internal static readonly string MasterTermCategoriesFile = "master_term_categories.csv";
        internal static readonly string ImageTagsFile = "image_tags.csv";
        public static readonly KeywordSnapshot Empty = new(new HashSet<string>(StringComparer.OrdinalIgnoreCase), new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase), new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase), new PhraseMatcher(new HashSet<string>(StringComparer.OrdinalIgnoreCase)), new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase), new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase), new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase), new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        public KeywordSnapshot(HashSet<string> flat, Dictionary<string, HashSet<string>> sources, Dictionary<string, List<string>> byCategory, PhraseMatcher matcher, Dictionary<string, string> synonymToMaster, Dictionary<string, bool> unwantedWords, HashSet<string> allMasterTerms, Dictionary<string, string> masterTermCategories, HashSet<string> imageTags)
        {
            FlatSet = flat;
            KeywordSources = sources;
            KeywordsByCategory = byCategory;
            Matcher = matcher;
            SynonymToMaster = synonymToMaster;
            UnwantedWords = unwantedWords;
            AllMasterTerms = allMasterTerms;
            MasterTermCategories = masterTermCategories;
            ImageTags = imageTags;

            // Pre-compute MasterToSynonyms inverse map
            var m2s = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in synonymToMaster)
            {
                if (!m2s.TryGetValue(kv.Value, out var list))
                    m2s[kv.Value] = list = new List<string>();
                list.Add(kv.Key);
            }
            MasterToSynonyms = m2s;

            // Pre-compute KeywordToCategories reverse map
            var kwCats = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in byCategory)
            {
                foreach (var kw in kv.Value ?? new List<string>())
                {
                    if (!kwCats.TryGetValue(kw, out var set))
                        kwCats[kw] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    set.Add(kv.Key);
                }
            }
            KeywordToCategories = kwCats;

            // Pre-compute normalized unwanted words for censorship checks
            var normUnwanted = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in unwantedWords)
            {
                normUnwanted[kv.Key] = NormalizeCensorship(kv.Key);
            }
            NormalizedUnwantedWords = normUnwanted;
        }

        internal static string NormalizeCensorship(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) return string.Empty;
            var sb = new StringBuilder(text.Length);
            foreach (var c in text.ToLowerInvariant())
            {
                var normalized = c switch
                {
                    '0' => 'o',
                    '1' => 'i',
                    '3' => 'e',
                    '4' => 'a',
                    '5' => 's',
                    '7' => 't',
                    '8' => 'b',
                    '@' => 'a',
                    '$' => 's',
                    '!' => 'i',
                    '+' => 't',
                    '-' or '_' or '.' or ',' or '*' or '#' or '~' or '`' or '\'' or '"' => '\0',
                    _ => c
                };
                if (normalized != '\0')
                    sb.Append(normalized);
            }
            return sb.ToString();
        }
    }
    public class KeywordCacheService : IKeywordCacheService
    {
        private readonly IServiceProvider _serviceProvider;
        private KeywordSnapshot _snapshot = KeywordSnapshot.Empty;
        private readonly SemaphoreSlim _refreshGate = new(1, 1);
        private Dictionary<string,
        List<string>> _keywordsByCategory = new(StringComparer.OrdinalIgnoreCase);
        private DateTimeOffset? _lastCsvWarmAt;
        private DateTimeOffset? _lastRefreshStartAt;
        private DateTimeOffset? _lastRefreshEndAt;
        private bool _lastRefreshSucceeded;
        private string? _lastRefreshError;
        public event Action<KeywordCacheStatus>? StatusChanged;
        private void PublishStatus()
        {
            try
            {
                StatusChanged?.Invoke(GetKeywordCacheStatus());
            }
            catch { }
        }
        private readonly string _csvDir;
        private static readonly HashSet<string> EnglishStopWords = new(StringComparer.OrdinalIgnoreCase) {
      "a",
      "and",
      "about",
      "above",
      "after",
      "again",
      "against",
      "all",
      "am",
      "an",
      "and",
      "any",
      "are",
      "aren't",
      "as",
      "at",
      "be",
      "because",
      "been",
      "before",
      "being",
      "below",
      "between",
      "both",
      "but",
      "by",
      "can",
      "cannot",
      "could",
      "couldn't",
      "did",
      "didn't",
      "do",
      "does",
      "doesn't",
      "doing",
      "don't",
      "down",
      "during",
      "each",
      "few",
      "for",
      "from",
      "further",
      "had",
      "hadn't",
      "has",
      "hasn't",
      "have",
      "haven't",
      "having",
      "he",
      "he'd",
      "he'll",
      "he's",
      "her",
      "here",
      "here's",
      "hers",
      "herself",
      "him",
      "himself",
      "his",
      "how",
      "how's",
      "i",
      "i'd",
      "i'll",
      "i'm",
      "i've",
      "if",
      "in",
      "into",
      "is",
      "isn't",
      "it",
      "it's",
      "its",
      "itself",
      "just",
      "let's",
      "me",
      "more",
      "most",
      "mustn't",
      "my",
      "myself",
      "no",
      "nor",
      "not",
      "of",
      "off",
      "on",
      "once",
      "only",
      "or",
      "other",
      "ought",
      "our",
      "ours",
      "ourselves",
      "out",
      "over",
      "own",
      "same",
      "she",
      "she'd",
      "she'll",
      "she's",
      "should",
      "shouldn't",
      "so",
      "some",
      "such",
      "than",
      "that",
      "that's",
      "the",
      "their",
      "theirs",
      "them",
      "themselves",
      "then",
      "there",
      "there's",
      "these",
      "they",
      "they'd",
      "they'll",
      "they're",
      "they've",
      "this",
      "those",
      "through",
      "to",
      "too",
      "under",
      "until",
      "up",
      "very",
      "was",
      "wasn't",
      "we",
      "we'd",
      "we'll",
      "we're",
      "we've",
      "were",
      "weren't",
      "what",
      "what's",
      "when",
      "when's",
      "where",
      "where's",
      "which",
      "while",
      "who",
      "who's",
      "whom",
      "why",
      "why's",
      "will",
      "with",
      "won't",
      "would",
      "wouldn't",
      "you",
      "you'd",
      "you'll",
      "you're",
      "you've",
      "your",
      "yours",
      "yourself",
      "yourselves"
    };
        public KeywordCacheService(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            var appDir = AppContext.BaseDirectory;
            _csvDir = Path.Combine(appDir, "keyword_cache");
            Directory.CreateDirectory(_csvDir);
            Console.WriteLine($"[KeywordCacheService] CSV dir: {_csvDir}");
        }
        public IReadOnlyDictionary<string,
        List<string>> GetKeywordsByCategory()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.KeywordsByCategory;
        }
        public HashSet<string> GetFlatKeywordSet()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.FlatSet;
        }
        public IReadOnlyDictionary<string,
        IReadOnlyCollection<string>> GetKeywordSources()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.KeywordSources.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyCollection<string>)kvp.Value.ToList(), StringComparer.OrdinalIgnoreCase);
        }
        public IReadOnlyCollection<string> GetSourcesFor(string keyword)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.KeywordSources.TryGetValue(keyword, out
            var set) ? (IReadOnlyCollection<string>)set : Array.Empty<string>();
        }
        public IReadOnlyCollection<string> GetSynonymsForMaster(string master)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(master)) return Array.Empty<string>();
            var snap = _snapshot;
            return snap.MasterToSynonyms.TryGetValue(master, out var list)
                ? list : (IReadOnlyCollection<string>)Array.Empty<string>();
        }
        public List<string> Search(string text)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(text)) return new();
            var snap = _snapshot;
            var matches = snap.Matcher.FindMatches(text);
            matches = RemoveRedundantSubstrings(matches);
            return matches.ToList();
        }
        public List<SearchHit> SearchWithSources(string text)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(text)) return new();
            var snap = _snapshot;
            var matches = snap.Matcher.FindMatches(text);
            matches = RemoveRedundantSubstrings(matches);
            var hits = new List<SearchHit>(matches.Count);
            foreach (var m in matches)
            {
                var sources = snap.KeywordSources.TryGetValue(m, out
                var set) ? (IReadOnlyCollection<string>)set.ToList() : Array.Empty<string>();
                if (sources.Count == 0) continue;
                var canonical = Canonicalize(m);
                hits.Add(new SearchHit(canonical, sources));
            }
            return hits;
        }
        public List<string> SearchForClassification(string text)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(text)) return new();
            var snap = _snapshot;
            var raw = snap.Matcher.FindMatches(text);
            raw = RemoveRedundantSubstrings(raw);
            var combined = new List<string>(raw.Count * 2);
            combined.AddRange(raw);
            foreach (var r in raw)
            {
                if (snap.SynonymToMaster.TryGetValue(r, out
                var master) && !string.IsNullOrWhiteSpace(master)) combined.Add(master);
            }
            combined = combined.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            combined = RemoveRedundantSubstrings(combined);
            return combined;
        }
        public string Canonicalize(string term)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(term)) return term;
            var snap = _snapshot;
            return snap.SynonymToMaster.TryGetValue(term, out
            var master) && !string.IsNullOrWhiteSpace(master) ? master : term;
        }
        public IReadOnlyDictionary<string, bool> GetUnwantedWords()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.UnwantedWords;
        }
        public bool IsUnwantedWord(string word)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(word)) return false;
            return _snapshot.UnwantedWords.ContainsKey(word.Trim());
        }
        public bool IsSuperBlacklistMatch(string text)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(text)) return false;
            var normalizedText = text.ToLowerInvariant();
            foreach (var kv in _snapshot.UnwantedWords)
            {
                if (kv.Value && normalizedText.Contains(kv.Key.ToLowerInvariant()))
                    return true;
            }
            return false;
        }
        public IReadOnlyCollection<string> GetAllSynonymMasters()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.AllMasterTerms;
        }
        public string? GetSynonymCategory(string keyword)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(keyword)) return null;
            var snap = _snapshot;
            var canonical = Canonicalize(keyword);
            return snap.MasterTermCategories.TryGetValue(canonical, out var category) ? category : null;
        }
        public IReadOnlyDictionary<string, HashSet<string>> GetKeywordToCategories()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.KeywordToCategories;
        }
        public IReadOnlyCollection<string> GetImageTags()
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            return _snapshot.KeywordSources.Keys;
        }
        public List<TagWithOrigin> SearchAllWithOrigin(string query)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(query)) return new();
            var q = query.Trim();
            var snap = _snapshot;
            var results = new List<TagWithOrigin>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var kv in snap.KeywordSources)
            {
                var keyword = kv.Key;
                if (!keyword.Contains(q, StringComparison.OrdinalIgnoreCase)) continue;
                if (!seen.Add(keyword)) continue;
                var origin = ResolveOrigin(keyword, kv.Value, snap);
                results.Add(new TagWithOrigin { Tag = keyword, Origin = origin });
            }
            results.Sort((a, b) => string.Compare(a.Tag, b.Tag, StringComparison.OrdinalIgnoreCase));
            return results;
        }
        private static string ResolveOrigin(string keyword, HashSet<string> sources, KeywordSnapshot snap)
        {
            // First check if this keyword (or its master) has a category from the synonyms system
            if (snap.MasterTermCategories.TryGetValue(keyword, out var cat))
                return cat;
            if (snap.SynonymToMaster.TryGetValue(keyword, out var master)
                && snap.MasterTermCategories.TryGetValue(master, out var masterCat))
                return masterCat;
            // Fall back to the source label from the DB table it was loaded from
            // Pick the most specific non-synonym source
            foreach (var src in sources)
            {
                if (src.StartsWith("synonym:", StringComparison.OrdinalIgnoreCase)) continue;
                if (src == "synonym_master") continue;
                return FormatOrigin(src);
            }
            // If only synonym sources, use the first one
            return sources.Count > 0 ? FormatOrigin(sources.First()) : "unknown";
        }
        private static string FormatOrigin(string source)
        {
            // Convert source labels like "movie:director" → "Director", "lighting_type" → "Lighting Type"
            var label = source;
            if (label.Contains(':'))
                label = label.Substring(label.IndexOf(':') + 1);
            // Convert snake_case to Title Case
            return string.Join(' ', label.Split('_')
                .Select(w => w.Length > 0 ? char.ToUpperInvariant(w[0]) + w.Substring(1).ToLowerInvariant() : w));
        }
        public CensorshipCheckResult CheckCensorship(string text)
        {
            EnsureWarmOrCsvThenBackgroundRefresh();
            if (string.IsNullOrWhiteSpace(text))
            {
                return new CensorshipCheckResult
                {
                    RejectedForCensorship = false,
                    SuperBlackList = false,
                    MatchedWord = null,
                    OriginalPhrase = null
                };
            }

            var snap = _snapshot;
            var originalText = text;
            var normalizedText = KeywordSnapshot.NormalizeCensorship(text);
            var lowerText = text.ToLowerInvariant();

            foreach (var kv in snap.UnwantedWords)
            {
                var unwantedWord = kv.Key;
                var isSuperBlacklist = kv.Value;
                var normalizedUnwanted = snap.NormalizedUnwantedWords.TryGetValue(unwantedWord, out var cached)
                    ? cached : KeywordSnapshot.NormalizeCensorship(unwantedWord);
                var lowerUnwanted = unwantedWord.ToLowerInvariant();

                bool foundMatch = false;
                string? matchedPhrase = null;

                if (isSuperBlacklist)
                {
                    if (normalizedText.Contains(normalizedUnwanted, StringComparison.OrdinalIgnoreCase))
                    {
                        foundMatch = true;
                        matchedPhrase = FindOriginalPhrase(originalText, unwantedWord);
                    }
                }
                else
                {
                    if (IsWholeWordMatch(normalizedText, normalizedUnwanted) || 
                        IsWholeWordMatch(lowerText, lowerUnwanted))
                    {
                        foundMatch = true;
                        matchedPhrase = FindOriginalPhrase(originalText, unwantedWord);
                    }
                }

                if (foundMatch)
                {
                    if (IsPartOfKnownSynonymMaster(originalText, unwantedWord, snap.AllMasterTerms))
                    {
                        continue;
                    }

                    return new CensorshipCheckResult
                    {
                        RejectedForCensorship = true,
                        SuperBlackList = isSuperBlacklist,
                        MatchedWord = unwantedWord,
                        OriginalPhrase = matchedPhrase
                    };
                }
            }

            return new CensorshipCheckResult
            {
                RejectedForCensorship = false,
                SuperBlackList = false,
                MatchedWord = null,
                OriginalPhrase = null
            };
        }
        private static string NormalizeForCensorshipCheck(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) return string.Empty;
            
            var sb = new StringBuilder(text.Length);
            foreach (var c in text.ToLowerInvariant())
            {
                var normalized = c switch
                {
                    '0' => 'o',
                    '1' => 'i',
                    '3' => 'e',
                    '4' => 'a',
                    '5' => 's',
                    '7' => 't',
                    '8' => 'b',
                    '@' => 'a',
                    '$' => 's',
                    '!' => 'i',
                    '+' => 't',
                    '-' or '_' or '.' or ',' or '*' or '#' or '~' or '`' or '\'' or '"' => '\0',
                    _ => c
                };
                
                if (normalized != '\0')
                    sb.Append(normalized);
            }
            
            return sb.ToString();
        }
        private static bool IsWholeWordMatch(string text, string word)
        {
            if (string.IsNullOrWhiteSpace(text) || string.IsNullOrWhiteSpace(word))
                return false;

            var index = 0;
            while ((index = text.IndexOf(word, index, StringComparison.OrdinalIgnoreCase)) >= 0)
            {
                var beforeOk = index == 0 || !char.IsLetterOrDigit(text[index - 1]);
                var afterIndex = index + word.Length;
                var afterOk = afterIndex >= text.Length || !char.IsLetterOrDigit(text[afterIndex]);

                if (beforeOk && afterOk)
                    return true;

                index++;
            }

            return false;
        }
        private static string? FindOriginalPhrase(string originalText, string unwantedWord)
        {
            var lowerText = originalText.ToLowerInvariant();
            var lowerWord = unwantedWord.ToLowerInvariant();
            
            var index = lowerText.IndexOf(lowerWord, StringComparison.OrdinalIgnoreCase);
            if (index >= 0)
            {
                return originalText.Substring(index, unwantedWord.Length);
            }
            
            var normalizedText = NormalizeForCensorshipCheck(originalText);
            var normalizedWord = NormalizeForCensorshipCheck(unwantedWord);
            
            index = normalizedText.IndexOf(normalizedWord, StringComparison.OrdinalIgnoreCase);
            if (index >= 0)
            {
                var start = Math.Max(0, index - 5);
                var length = Math.Min(unwantedWord.Length + 10, originalText.Length - start);
                return originalText.Substring(start, length).Trim();
            }
            
            return null;
        }
        private static bool IsPartOfKnownSynonymMaster(string text, string unwantedWord, HashSet<string> allMasterTerms)
        {
            var lowerText = text.ToLowerInvariant();
            var lowerUnwanted = unwantedWord.ToLowerInvariant();

            foreach (var master in allMasterTerms)
            {
                var lowerMaster = master.ToLowerInvariant();
                
                if (!lowerMaster.Contains(lowerUnwanted, StringComparison.OrdinalIgnoreCase))
                    continue;
                
                if (lowerMaster.Equals(lowerUnwanted, StringComparison.OrdinalIgnoreCase))
                    continue;
                
                if (lowerText.Contains(lowerMaster, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }
        public KeywordCacheStatus GetKeywordCacheStatus()
        {
            var snap = _snapshot;
            return new KeywordCacheStatus
            {
                LastCsvWarmAt = _lastCsvWarmAt,
                LastRefreshStartAt = _lastRefreshStartAt,
                LastRefreshEndAt = _lastRefreshEndAt,
                IsRefreshRunning = (_refreshGate.CurrentCount == 0),
                LastRefreshSucceeded = _lastRefreshSucceeded,
                LastRefreshError = _lastRefreshError,
                FlatCount = snap.FlatSet.Count,
                SourceKeys = snap.KeywordSources.Count,
                CategoryCount = snap.KeywordsByCategory.Count,
                SynonymCount = snap.SynonymToMaster.Count,
                UnwantedWordsCount = snap.UnwantedWords.Count,
                SuperBlacklistCount = snap.UnwantedWords.Count(kv => kv.Value)
            };
        }
        private void EnsureWarmOrCsvThenBackgroundRefresh()
        {
            if (!ReferenceEquals(_snapshot, KeywordSnapshot.Empty)) return;
            if (TryWarmFromCsv(out
            var warmed))
            {
                Interlocked.Exchange(ref _snapshot, warmed);
                _lastCsvWarmAt = DateTimeOffset.UtcNow;
                PublishStatus();
                Console.WriteLine($"[KeywordCacheService] CSV re-warm complete. Flat={warmed.FlatSet.Count}, Syn={warmed.SynonymToMaster.Count}");
            }
            else
            {
                Console.WriteLine("[KeywordCacheService] CSV re-warm unavailable; still Empty until refresh.");
            }
            _ = TriggerRefreshInBackground();
        }
        public async Task RefreshAsync()
        {
            await _refreshGate.WaitAsync().ConfigureAwait(false);
            try
            {
                _lastRefreshStartAt = DateTimeOffset.UtcNow;
                _lastRefreshError = null;
                _lastRefreshSucceeded = false;
                PublishStatus();
                Console.WriteLine("[KeywordCache] Refresh (public) started…");
                await RefreshCoreAsync().ConfigureAwait(false);
                _lastRefreshSucceeded = true;
                Console.WriteLine("[KeywordCache] Refresh (public) completed.");
            }
            catch (Exception ex)
            {
                _lastRefreshError = ex.ToString();
                _lastRefreshSucceeded = false;
                Console.Error.WriteLine($"[KeywordCache] Refresh (public) FAILED: {ex}");
                throw;
            }
            finally
            {
                _lastRefreshEndAt = DateTimeOffset.UtcNow;
                PublishStatus();
                _refreshGate.Release();
            }
        }
        private async Task TriggerRefreshInBackground()
        {
            if (!_refreshGate.Wait(0))
            {
                Console.WriteLine("[KeywordCache] Refresh already running; skip trigger.");
                return;
            }
            try
            {
                _lastRefreshStartAt = DateTimeOffset.UtcNow;
                _lastRefreshError = null;
                _lastRefreshSucceeded = false;
                PublishStatus();
                Console.WriteLine("[KeywordCache] Refresh (bg) started…");
                await RefreshCoreAsync().ConfigureAwait(false);
                _lastRefreshSucceeded = true;
                Console.WriteLine("[KeywordCache] Refresh (bg) completed.");
            }
            catch (Exception ex)
            {
                _lastRefreshError = ex.ToString();
                _lastRefreshSucceeded = false;
                Console.Error.WriteLine($"[KeywordCache] Refresh (bg) FAILED: {ex}");
            }
            finally
            {
                _lastRefreshEndAt = DateTimeOffset.UtcNow;
                PublishStatus();
                _refreshGate.Release();
            }
        }
        private async Task RefreshCoreAsync()
        {
            using
            var scope = _serviceProvider.CreateScope();
            var conn = scope.ServiceProvider.GetRequiredService<NpgsqlConnection>();
            var newSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var newSources = new Dictionary<string,
            HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var newByCategory = new Dictionary<string,
            List<string>>(StringComparer.OrdinalIgnoreCase);
            void AddKeywordLocal(string keyword, string source)
            {
                if (string.IsNullOrWhiteSpace(keyword)) return;
                var trimmed = keyword.Trim();
                if (trimmed.Length == 0) return;
                newSet.Add(trimmed);
                if (!newSources.TryGetValue(trimmed, out
                var srcSet)) newSources[trimmed] = srcSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                srcSet.Add(source);
            }
            async Task AddTagsToSet(string table, string column, string sourceLabel)
            {
                var sw = Stopwatch.StartNew();
                var tags = await FetchTags(conn, table, column);
                foreach (var tag in tags) AddKeywordLocal(tag, sourceLabel);
                sw.Stop();
                Console.WriteLine($"{table}: {sourceLabel} {tags.Count} records read in {sw.ElapsedMilliseconds} ms");
            }
            async Task AddMoviesTagsToSet()
            {
                var sw = Stopwatch.StartNew();
                var rows = await FetchMovieFieldsByColumn(conn);
                foreach (var (value, fieldName) in rows)
                {
                    var v = value?.Trim();
                    if (!string.IsNullOrEmpty(v)) AddKeywordLocal(v, $"movie:{fieldName}");
                }
                sw.Stop();
                Console.WriteLine($"Movies: {rows.Count} records read in {sw.ElapsedMilliseconds} ms");
            }
            async Task AddImagesTagsToSet()
            {
                var sw = Stopwatch.StartNew();
                var rows = await FetchImageFieldsByColumn(conn);
                foreach (var (value, fieldName) in rows)
                {
                    var v = value?.Trim();
                    if (!string.IsNullOrEmpty(v)) AddKeywordLocal(v, $"image:{fieldName}");
                }
                sw.Stop();
                Console.WriteLine($"Images: {rows.Count} records read in {sw.ElapsedMilliseconds} ms");
            }
            await AddTagsToSet("frl_join_images_time_of_day", "time_of_day", "time_of_day");
            await AddTagsToSet("frl_join_images_lighting_type", "lighting_type", "lighting_type");
            await AddTagsToSet("frl_join_images_vfx_backing", "vfx_backing", "vfx_backing");
            await AddTagsToSet("frl_join_images_color", "color", "color");
            await AddTagsToSet("frl_join_images_shot_type", "shot_type", "shot_type");
            await AddTagsToSet("frl_join_images_lighting", "lighting", "lighting");
            foreach (var s in new[] {
        "Ultra Wide / Fisheye",
        "Wide",
        "Medium",
        "Long Lens",
        "Telephoto"
      }) AddKeywordLocal(s, "lens size");
            foreach (var s in new[] {
        "Center",
        "Left heavy",
        "Right heavy",
        "Balanced",
        "Symmetrical",
        "Short side"
      }) AddKeywordLocal(s, "composition");
            await AddImagesTagsToSet();
            await AddMoviesTagsToSet();
            // Fetch unique image tags for in-memory lookup
            var imageTags = await FetchImageTags(conn);
            foreach (var tag in imageTags) AddKeywordLocal(tag, "image_tag");
            Console.WriteLine($"[KeywordCache] Image tags loaded: {imageTags.Count}");
            if (_keywordsByCategory.Count > 0) newByCategory = new Dictionary<string,
            List<string>>(_keywordsByCategory, StringComparer.OrdinalIgnoreCase);
            var (synonymToMaster, allMasterTerms) = await FetchSynonymToMasterMapAndMasters(conn);
            Console.WriteLine($"[KeywordCache] Synonyms loaded: {synonymToMaster.Count}, Master terms loaded: {allMasterTerms.Count}");
            
            // Add all master terms to the keyword set so they are searchable
            foreach (var master in allMasterTerms)
            {
                if (string.IsNullOrWhiteSpace(master)) continue;
                AddKeywordLocal(master, $"synonym_master");
            }
            
            foreach (var kv in synonymToMaster)
            {
                var synonym = kv.Key;
                var master = kv.Value;
                if (string.IsNullOrWhiteSpace(synonym) || string.IsNullOrWhiteSpace(master)) continue;
                AddKeywordLocal(synonym, $"synonym:{master}");
                if (newSources.TryGetValue(master, out
                var masterSources))
                {
                    if (!newSources.TryGetValue(synonym, out
                    var synSources)) newSources[synonym] = synSources = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    foreach (var src in masterSources) synSources.Add(src);
                }
            }
            var unwantedWords = await FetchUnwantedWords(conn);
            var superBlacklistCount = unwantedWords.Count(kv => kv.Value);
            Console.WriteLine($"[KeywordCache] Unwanted words loaded: {unwantedWords.Count}, Super blacklist: {superBlacklistCount}");
            
            var masterTermCategories = await FetchMasterTermCategories(conn);
            Console.WriteLine($"[KeywordCache] Master term categories loaded: {masterTermCategories.Count}");
            
            var newMatcher = new PhraseMatcher(newSet);
            var newSnapshot = new KeywordSnapshot(newSet, newSources, newByCategory, newMatcher, synonymToMaster, unwantedWords, allMasterTerms, masterTermCategories, imageTags);
            SaveSnapshotToCsv(newSnapshot);
            Interlocked.Exchange(ref _snapshot, newSnapshot);
            PublishStatus();
            Console.WriteLine($"[KeywordCache] Snapshot swap: flat={_snapshot.FlatSet.Count}, sources={_snapshot.KeywordSources.Count}, cats={_snapshot.KeywordsByCategory.Count}, syn={_snapshot.SynonymToMaster.Count}, unwanted={_snapshot.UnwantedWords.Count}, masters={_snapshot.AllMasterTerms.Count}, masterCats={_snapshot.MasterTermCategories.Count}, imageTags={_snapshot.ImageTags.Count}");
        }
        private static async Task<(Dictionary<string, string> SynonymToMaster, HashSet<string> AllMasterTerms)> FetchSynonymToMasterMapAndMasters(NpgsqlConnection conn)
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            var allMasters = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            // First, fetch all master terms (including those without synonyms)
            const string masterSql = @"SELECT master_term FROM frl.frl_keywords_synonyms_master WHERE is_included = true;";
            using (var masterCmd = new NpgsqlCommand(masterSql, conn))
            using (var masterReader = await masterCmd.ExecuteReaderAsync())
            {
                while (await masterReader.ReadAsync())
                {
                    if (masterReader.IsDBNull(0)) continue;
                    var master = masterReader.GetString(0)?.Trim();
                    if (!string.IsNullOrWhiteSpace(master))
                        allMasters.Add(master);
                }
            }
            
            // Then, fetch synonym-to-master mappings
            const string sql = @"SELECT m.master_term, s.synonym_term FROM frl.frl_keywords_synonyms_master m JOIN frl.frl_keywords_synonyms s ON s.master_id = m.id WHERE m.is_included = true AND s.is_included = true;";
            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0) || reader.IsDBNull(1)) continue;
                var master = reader.GetString(0)?.Trim();
                var syn = reader.GetString(1)?.Trim();
                if (string.IsNullOrWhiteSpace(master) || string.IsNullOrWhiteSpace(syn)) continue;
                map[syn] = master;
            }
            return (map, allMasters);
        }
        private static async Task<Dictionary<string, bool>> FetchUnwantedWords(NpgsqlConnection conn)
        {
            var unwantedWords = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            
            const string sql = @"SELECT phrase, is_super_blacklist FROM frl.frl_keywords_unwanted_words;";
            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            
            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0)) continue;
                var phrase = reader.GetString(0)?.Trim();
                if (string.IsNullOrWhiteSpace(phrase)) continue;
                
                var isSuperBlacklist = !reader.IsDBNull(1) && reader.GetBoolean(1);
                unwantedWords[phrase] = isSuperBlacklist;
            }
            
            return unwantedWords;
        }
        private static async Task<HashSet<string>> FetchImageTags(NpgsqlConnection conn)
        {
            var tags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            const string sql = @"SELECT DISTINCT tag FROM frl_join_images_tags WHERE tag IS NOT NULL;";
            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0)) continue;
                var tag = reader.GetString(0)?.Trim();
                if (!string.IsNullOrWhiteSpace(tag))
                    tags.Add(tag);
            }
            return tags;
        }
        private static async Task<Dictionary<string, string>> FetchMasterTermCategories(NpgsqlConnection conn)
        {
            var categories = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            const string sql = @"
                SELECT m.master_term, c.category_name 
                FROM frl.frl_keywords_synonyms_master m 
                JOIN frl.frl_keywords_synonyms_category c ON m.category_id = c.id 
                WHERE m.is_included = true AND m.category_id IS NOT NULL;";
            using var cmd = new NpgsqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            
            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0) || reader.IsDBNull(1)) continue;
                var masterTerm = reader.GetString(0)?.Trim();
                var categoryName = reader.GetString(1)?.Trim();
                if (string.IsNullOrWhiteSpace(masterTerm) || string.IsNullOrWhiteSpace(categoryName)) continue;
                categories[masterTerm] = categoryName;
            }
            
            return categories;
        }
        private bool TryWarmFromCsv(out KeywordSnapshot snapshot)
        {
            snapshot = KeywordSnapshot.Empty;
            try
            {
                var flatPath = Path.Combine(_csvDir, KeywordSnapshot.FlatFile);
                var sourcesPath = Path.Combine(_csvDir, KeywordSnapshot.SourcesFile);
                var byCatPath = Path.Combine(_csvDir, KeywordSnapshot.ByCategoryFile);
                var synPath = Path.Combine(_csvDir, KeywordSnapshot.SynonymsFile);
                var unwantedPath = Path.Combine(_csvDir, KeywordSnapshot.UnwantedWordsFile);
                var masterTermsPath = Path.Combine(_csvDir, KeywordSnapshot.MasterTermsFile);
                var masterTermCategoriesPath = Path.Combine(_csvDir, KeywordSnapshot.MasterTermCategoriesFile);
                if (!File.Exists(flatPath) || !File.Exists(sourcesPath) || !File.Exists(byCatPath)) return false;
                var flat = LoadFlatSet(flatPath);
                var sources = LoadKeywordSources(sourcesPath);
                var byCat = LoadKeywordsByCategory(byCatPath);
                var synonyms = File.Exists(synPath) ? LoadSynonyms(synPath) : new Dictionary<string,
                string>(StringComparer.OrdinalIgnoreCase);
                var unwantedWords = File.Exists(unwantedPath) 
                    ? LoadUnwantedWords(unwantedPath) 
                    : new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                var masterTerms = File.Exists(masterTermsPath) 
                    ? LoadMasterTerms(masterTermsPath) 
                    : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                var masterTermCategories = File.Exists(masterTermCategoriesPath)
                    ? LoadMasterTermCategories(masterTermCategoriesPath)
                    : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var imageTagsPath = Path.Combine(_csvDir, KeywordSnapshot.ImageTagsFile);
                var imageTags = File.Exists(imageTagsPath)
                    ? LoadImageTags(imageTagsPath)
                    : new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                if (flat.Count == 0) return false;
                var matcher = new PhraseMatcher(flat);
                snapshot = new KeywordSnapshot(flat, sources, byCat, matcher, synonyms, unwantedWords, masterTerms, masterTermCategories, imageTags);
                return true;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[KeywordCacheService] CSV warm failed: {ex}");
                return false;
            }
        }
        private static HashSet<string> LoadFlatSet(string path)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                var s = line.Trim();
                if (s.Length == 0) continue;
                if (s.Equals("keyword", StringComparison.OrdinalIgnoreCase)) continue;
                set.Add(Uncsv(s));
            }
            return set;
        }
        private static Dictionary<string,
        HashSet<string>> LoadKeywordSources(string path)
        {
            var dict = new Dictionary<string,
            HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("keyword,source", StringComparison.OrdinalIgnoreCase)) continue;
                var (a, b) = SplitCsv2(line);
                if (string.IsNullOrEmpty(a)) continue;
                a = Uncsv(a);
                b = Uncsv(b);
                if (!dict.TryGetValue(a, out
                var set)) dict[a] = set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                set.Add(b);
            }
            return dict;
        }
        private static Dictionary<string,
        List<string>> LoadKeywordsByCategory(string path)
        {
            var map = new Dictionary<string,
            List<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("category,keyword", StringComparison.OrdinalIgnoreCase)) continue;
                var (a, b) = SplitCsv2(line);
                if (string.IsNullOrEmpty(a) || string.IsNullOrEmpty(b)) continue;
                var cat = Uncsv(a);
                var kw = Uncsv(b);
                if (!map.TryGetValue(cat, out
                var list)) map[cat] = list = new List<string>();
                list.Add(kw);
            }
            return map;
        }
        private static Dictionary<string,
        string> LoadSynonyms(string path)
        {
            var map = new Dictionary<string,
            string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("synonym,master", StringComparison.OrdinalIgnoreCase)) continue;
                var (a, b) = SplitCsv2(line);
                if (string.IsNullOrWhiteSpace(a) || string.IsNullOrWhiteSpace(b)) continue;
                map[Uncsv(a)] = Uncsv(b);
            }
            return map;
        }
        private static Dictionary<string, bool> LoadUnwantedWords(string path)
        {
            var unwantedWords = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("phrase,is_super_blacklist", StringComparison.OrdinalIgnoreCase)) continue;
                
                var (phrase, isSuperBlacklistStr) = SplitCsv2(line);
                if (string.IsNullOrWhiteSpace(phrase)) continue;
                
                phrase = Uncsv(phrase);
                var isSuperBlacklist = string.Equals(Uncsv(isSuperBlacklistStr), "true", StringComparison.OrdinalIgnoreCase);
                unwantedWords[phrase] = isSuperBlacklist;
            }
            
            return unwantedWords;
        }
        private static HashSet<string> LoadMasterTerms(string path)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                var s = line.Trim();
                if (s.Length == 0) continue;
                if (s.Equals("master_term", StringComparison.OrdinalIgnoreCase)) continue;
                set.Add(Uncsv(s));
            }
            return set;
        }
        private static HashSet<string> LoadImageTags(string path)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                var s = line.Trim();
                if (s.Length == 0) continue;
                if (s.Equals("tag", StringComparison.OrdinalIgnoreCase)) continue;
                set.Add(Uncsv(s));
            }
            return set;
        }
        private static Dictionary<string, string> LoadMasterTermCategories(string path)
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var line in File.ReadLines(path, Encoding.UTF8))
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.StartsWith("master_term,category", StringComparison.OrdinalIgnoreCase)) continue;
                var (a, b) = SplitCsv2(line);
                if (string.IsNullOrWhiteSpace(a) || string.IsNullOrWhiteSpace(b)) continue;
                map[Uncsv(a)] = Uncsv(b);
            }
            return map;
        }
        private static (string A, string B) SplitCsv2(string line)
        {
            string a,
            b;
            int i = 0;
            static string ParseField(string s, ref int i)
            {
                var sb = new StringBuilder();
                if (i < s.Length && s[i] == '"')
                {
                    i++;
                    while (i < s.Length)
                    {
                        if (s[i] == '"' && i + 1 < s.Length && s[i + 1] == '"')
                        {
                            sb.Append('"');
                            i += 2;
                        }
                        else if (s[i] == '"')
                        {
                            i++;
                            break;
                        }
                        else
                        {
                            sb.Append(s[i++]);
                        }
                    }
                }
                else
                {
                    while (i < s.Length && s[i] != ',') sb.Append(s[i++]);
                }
                if (i < s.Length && s[i] == ',') i++;
                return sb.ToString();
            }
            a = ParseField(line, ref i);
            b = ParseField(line, ref i);
            return (a, b);
        }
        private static string Csv(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "\"\"";
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }
        private static string Uncsv(string s)
        {
            s = s.Trim();
            if (s.Length >= 2 && s[0] == '"' && s[^1] == '"') return s.Substring(1, s.Length - 2).Replace("\"\"", "\"");
            return s;
        }
        private void SaveSnapshotToCsv(KeywordSnapshot snap)
        {
            try
            {
                Directory.CreateDirectory(_csvDir);
                var flatLines = new List<string>(snap.FlatSet.Count + 1) {
          "keyword"
        };
                flatLines.AddRange(snap.FlatSet.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).Select(Csv));
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.FlatFile), flatLines, Encoding.UTF8);
                var srcLines = new List<string> {
          "keyword,source"
        };
                foreach (var kv in snap.KeywordSources.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                {
                    var k = Csv(kv.Key);
                    foreach (var src in kv.Value.OrderBy(v => v, StringComparer.OrdinalIgnoreCase)) srcLines.Add($"{k},{Csv(src)}");
                }
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.SourcesFile), srcLines, Encoding.UTF8);
                var byCatLines = new List<string> {
          "category,keyword"
        };
                foreach (var kv in snap.KeywordsByCategory.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase))
                {
                    var cat = Csv(kv.Key);
                    foreach (var kw in (kv.Value ?? new List<string>()).OrderBy(v => v, StringComparer.OrdinalIgnoreCase)) byCatLines.Add($"{cat},{Csv(kw)}");
                }
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.ByCategoryFile), byCatLines, Encoding.UTF8);
                var synLines = new List<string> {
          "synonym,master"
        };
                foreach (var kv in snap.SynonymToMaster.OrderBy(k => k.Key, StringComparer.OrdinalIgnoreCase)) synLines.Add($"{Csv(kv.Key)},{Csv(kv.Value)}");
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.SynonymsFile), synLines, Encoding.UTF8);
                
                var unwantedLines = new List<string>(snap.UnwantedWords.Count + 1) { "phrase,is_super_blacklist" };
                foreach (var kv in snap.UnwantedWords.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
                {
                    unwantedLines.Add($"{Csv(kv.Key)},{(kv.Value ? "true" : "false")}");
                }
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.UnwantedWordsFile), unwantedLines, Encoding.UTF8);
                
                var masterTermsLines = new List<string>(snap.AllMasterTerms.Count + 1) { "master_term" };
                masterTermsLines.AddRange(snap.AllMasterTerms.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).Select(Csv));
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.MasterTermsFile), masterTermsLines, Encoding.UTF8);
                
                var masterTermCategoriesLines = new List<string>(snap.MasterTermCategories.Count + 1) { "master_term,category" };
                foreach (var kv in snap.MasterTermCategories.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase))
                {
                    masterTermCategoriesLines.Add($"{Csv(kv.Key)},{Csv(kv.Value)}");
                }
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.MasterTermCategoriesFile), masterTermCategoriesLines, Encoding.UTF8);
                
                var imageTagsLines = new List<string>(snap.ImageTags.Count + 1) { "tag" };
                imageTagsLines.AddRange(snap.ImageTags.OrderBy(x => x, StringComparer.OrdinalIgnoreCase).Select(Csv));
                File.WriteAllLines(Path.Combine(_csvDir, KeywordSnapshot.ImageTagsFile), imageTagsLines, Encoding.UTF8);
                
                Console.WriteLine($"[KeywordCacheService] Snapshot CSVs written to {_csvDir}");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[KeywordCacheService] Failed to write CSVs: {ex}");
            }
        }
        public static List<string> RemoveRedundantSubstrings(List<string> phrases)
        {
            if (phrases == null || phrases.Count == 0) return new();
            var normToOrig = new Dictionary<string,
            string>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in phrases)
            {
                if (string.IsNullOrWhiteSpace(p)) continue;
                var trimmed = p.Trim();
                var lower = trimmed.ToLowerInvariant();
                if (EnglishStopWords.Contains(lower)) continue;
                if (!normToOrig.TryGetValue(lower, out
                var existing)) normToOrig[lower] = trimmed;
                else if (trimmed.Length > existing.Length) normToOrig[lower] = trimmed;
            }
            var normalizedKeys = normToOrig.Keys.ToList();
            var result = new List<string>();
            foreach (var kv in normToOrig)
            {
                var lower = kv.Key;
                var original = kv.Value;
                bool isSub = normalizedKeys.Any(other => !string.Equals(other, lower, StringComparison.Ordinal) && other.Contains(lower, StringComparison.Ordinal));
                if (!isSub) result.Add(original);
            }
            return result;
        }
        private static async Task<List<string>> FetchTags(NpgsqlConnection conn, string tableName, string columnName)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            string sql = $"SELECT DISTINCT {columnName} FROM {tableName}";
            using
            var cmd = new NpgsqlCommand(sql, conn);
            using
            var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (reader.IsDBNull(0)) continue;
                string val = reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(val) && val != "--" && !val.Equals("null", StringComparison.OrdinalIgnoreCase) && !Regex.IsMatch(val, "^:+$"))
                {
                    result.Add(val.Trim());
                }
            }
            return result.ToList();
        }
        private static async Task<List<(string Value, string Field)>> FetchMovieFieldsByColumn(NpgsqlConnection conn)
        {
            var result = new List<(string, string)>();
            const string sql = @" SELECT media_type, title, director, cinematographer, production_designer, costume_designer, mv_artist, comm_brand FROM frl_movies WHERE title IS NULL OR title <> 'The Movie';";
            using
            var cmd = new NpgsqlCommand(sql, conn);
            using
            var reader = await cmd.ExecuteReaderAsync();
            string[] fieldNames = {
        "media_type",
        "title",
        "director",
        "cinematographer",
        "production_designer",
        "costume_designer",
        "mv_artist",
        "comm_brand"
      };
            while (await reader.ReadAsync())
            {
                for (int i = 0; i < fieldNames.Length; i++)
                {
                    if (reader.IsDBNull(i)) continue;
                    var field = reader.GetString(i);
                    var entries = field.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(e => e.Trim()).Where(e => !string.IsNullOrWhiteSpace(e) && !e.Contains("Movie", StringComparison.OrdinalIgnoreCase));
                    foreach (var e in entries) result.Add((e, fieldNames[i]));
                }
            }
            return result;
        }
        private static async Task<List<(string Value, string Field)>> FetchImageFieldsByColumn(NpgsqlConnection conn)
        {
            var result = new List<(string Value, string Field)>();
            const string sql = @"SELECT actors, int_ext, aspect_ratio FROM frl_images;";
            using
            var cmd = new NpgsqlCommand(sql, conn);
            using
            var reader = await cmd.ExecuteReaderAsync();
            string[] fieldNames = {
        "actors",
        "int_ext",
        "aspect_ratio"
      };
            var seenByField = new Dictionary<string,
            HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var f in fieldNames) seenByField[f] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            while (await reader.ReadAsync())
            {
                for (int i = 0; i < fieldNames.Length; i++)
                {
                    if (reader.IsDBNull(i)) continue;
                    var fieldRaw = reader.GetString(i);
                    var fieldName = fieldNames[i];
                    var entries = fieldRaw.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(e => e.Trim()).Where(e => !string.IsNullOrWhiteSpace(e));
                    foreach (var entry in entries)
                    {
                        if (seenByField[fieldName].Add(entry)) result.Add((entry, fieldName));
                    }
                }
            }
            return result;
        }
    } // ------------------ PhraseMatcher (unchanged) ------------------ 
    public class PhraseMatcher
    {
        private readonly HashSet<string> _phrases;
        private readonly List<string> _originals = new();
        private readonly Node _root = new();
        public PhraseMatcher(HashSet<string> phrases)
        {
            _phrases = phrases;
            var normToOrig = new Dictionary<string,
            string>(StringComparer.Ordinal);
            foreach (var p in _phrases)
            {
                if (string.IsNullOrWhiteSpace(p)) continue;
                var norm = NormalizeForMatch(p);
                if (norm.Length == 0) continue;
                if (!normToOrig.ContainsKey(norm)) normToOrig[norm] = p.Trim();
            }
            foreach (var (norm, orig) in normToOrig) AddPattern(norm, orig);
            BuildFailures();
        }
        public List<string> FindMatches(string text)
        {
            var results = new List<string>();
            if (string.IsNullOrWhiteSpace(text)) return results;
            var normText = NormalizeForMatch(text);
            var node = _root;
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < normText.Length; i++)
            {
                char c = normText[i];
                Node? next;
                while (node != _root && !node.Next.TryGetValue(c, out next)) node = node.Fail!;
                if (node.Next.TryGetValue(c, out next)) node = next;
                else node = _root;
                var outNode = node;
                while (outNode != null)
                {
                    int outIdx = outNode.OutputIndex;
                    if (outIdx >= 0)
                    {
                        int end = i;
                        int start = end - outNode.Depth + 1;
                        if (start >= 0 && IsWordBoundary(normText, start - 1) && IsWordBoundary(normText, end + 1))
                        {
                            var phrase = _originals[outIdx];
                            if (seen.Add(phrase)) results.Add(phrase);
                        }
                    }
                    outNode = outNode.Fail;
                }
            }
            return results;
        }
        private static string NormalizeForMatch(string s)
        {
            var span = s.AsSpan();
            var sb = new StringBuilder(span.Length);
            bool lastSpace = false;
            for (int i = 0; i < span.Length; i++)
            {
                char c = char.ToLowerInvariant(span[i]);
                if (char.IsWhiteSpace(c))
                {
                    if (!lastSpace)
                    {
                        sb.Append(' ');
                        lastSpace = true;
                    }
                }
                else
                {
                    sb.Append(c);
                    lastSpace = false;
                }
            }
            return sb.ToString().Trim();
        }
        private static bool IsWordBoundary(string text, int index)
        {
            if (index < 0 || index >= text.Length) return true;
            return !char.IsLetterOrDigit(text[index]);
        }
        private void AddPattern(string normalizedLower, string originalPhrase)
        {
            var node = _root;
            foreach (var ch in normalizedLower)
            {
                if (!node.Next.TryGetValue(ch, out
                var next))
                {
                    next = new Node
                    {
                        Depth = node.Depth + 1
                    };
                    node.Next[ch] = next;
                }
                node = next;
            }
            if (node.OutputIndex < 0)
            {
                node.OutputIndex = _originals.Count;
                _originals.Add(originalPhrase);
            }
        }
        private void BuildFailures()
        {
            var q = new Queue<Node>();
            foreach (var child in _root.Next.Values)
            {
                child.Fail = _root;
                q.Enqueue(child);
            }
            while (q.Count > 0)
            {
                var r = q.Dequeue();
                foreach (var kv in r.Next)
                {
                    char a = kv.Key;
                    var u = kv.Value;
                    Node? f = r.Fail;
                    Node? link = null;
                    while (f != null && !f.Next.TryGetValue(a, out link)) f = f.Fail;
                    u.Fail = link ?? _root;
                    q.Enqueue(u);
                }
            }
        }
        private sealed class Node
        {
            public readonly Dictionary<char,
            Node> Next = new();
            public Node? Fail;
            public int Depth;
            public int OutputIndex = -1;
        }
    }
}
