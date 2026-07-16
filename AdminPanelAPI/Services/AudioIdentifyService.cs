using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using AdminPanelAPI.Models;

namespace AdminPanelAPI.Services
{
    public class AudioIdentifyService : IAudioIdentifyService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<AudioIdentifyService> _logger;
        private readonly string _musicApiBaseUrl;

        // Audio-input chat model that can "listen" to the clip.
        private const string AudioModel = "gpt-audio";

        // Web-search reasoning model used to turn the audio model's
        // composer/style read into a specific soundtrack cue title.
        private const string WebSearchModel = "gpt-4o";

        public AudioIdentifyService(
            IHttpClientFactory httpClientFactory,
            IConfiguration configuration,
            ILogger<AudioIdentifyService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _configuration = configuration;
            _logger = logger;
            _musicApiBaseUrl =
                (_configuration["MusicIdentification:MusicApiBaseUrl"]
                 ?? "http://localhost:8000").TrimEnd('/');
        }

        public async Task<AudioIdentifySuggestion> IdentifyAsync(
            string r2Key,
            double start,
            double end,
            string currentTitle,
            string? currentArtist,
            string movieTitle,
            int? movieYear,
            IReadOnlyList<SoundtrackCue> soundtrackCues,
            CancellationToken cancellationToken)
        {
            var apiKey = _configuration["OpenAI:ApiKey"]
                ?? _configuration["OPENAI_API_KEY"];
            if (string.IsNullOrWhiteSpace(apiKey))
                return new AudioIdentifySuggestion { Error = "OpenAI key not configured." };

            string audioB64;
            try
            {
                audioB64 = await ExtractClipAsync(r2Key, start, end, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Audio clip extraction failed for {R2Key}.", r2Key);
                return new AudioIdentifySuggestion { Error = "Couldn't extract the clip audio." };
            }

            AudioIdentifySuggestion suggestion;
            try
            {
                suggestion = await AskAudioModelAsync(
                    apiKey, audioB64, currentTitle, currentArtist,
                    movieTitle, movieYear, soundtrackCues, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Audio identification call failed.");
                return new AudioIdentifySuggestion { Error = "The audio model call failed." };
            }

            // Second step: the audio model reliably hears the composer/style but
            // not always the exact cue title. When it already recognised a title
            // (often now that it's given the film's tracklist as a hint), keep it
            // as a genuine aural recognition. Otherwise, when it heard something
            // usable (a performer/composer or an instrumental score cue), use a
            // web-search reasoning pass over the film's soundtrack/score to
            // propose a best-guess title — the way ChatGPT gets it — flagged
            // unverified. Advisory only; failures leave the audio suggestion
            // untouched.
            if (suggestion.Error == null &&
                string.IsNullOrWhiteSpace(suggestion.Title) &&
                (!string.IsNullOrWhiteSpace(suggestion.Artist) || suggestion.IsScoreCue))
            {
                try
                {
                    await RefineTitleWithWebSearchAsync(
                        apiKey, suggestion, currentTitle, currentArtist,
                        movieTitle, movieYear, soundtrackCues, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Web-search title refinement failed.");
                }
            }

            return suggestion;
        }

        private async Task<string> ExtractClipAsync(
            string r2Key, double start, double end, CancellationToken cancellationToken)
        {
            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(120);

            var url = $"{_musicApiBaseUrl}/extract-clip"
                + $"?r2_key={Uri.EscapeDataString(r2Key)}"
                + $"&start={start.ToString(System.Globalization.CultureInfo.InvariantCulture)}"
                + $"&end={end.ToString(System.Globalization.CultureInfo.InvariantCulture)}";

            using var resp = await client.PostAsync(url, null, cancellationToken);
            resp.EnsureSuccessStatusCode();
            using var stream = await resp.Content.ReadAsStreamAsync(cancellationToken);
            using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
            return doc.RootElement.GetProperty("audio_b64").GetString()
                ?? throw new InvalidOperationException("extract-clip returned no audio.");
        }

        private async Task<AudioIdentifySuggestion> AskAudioModelAsync(
            string apiKey,
            string audioB64,
            string currentTitle,
            string? currentArtist,
            string movieTitle,
            int? movieYear,
            IReadOnlyList<SoundtrackCue> soundtrackCues,
            CancellationToken cancellationToken)
        {
            var yearText = movieYear.HasValue ? $" ({movieYear})" : "";
            // The film's known soundtrack/score cues, offered as a HINT (not a
            // whitelist): if the clip clearly matches one, naming it lets the model
            // pick the right real cue; unlisted needle-drops must still identify
            // freely.
            var cuesHint = "";
            if (soundtrackCues.Count > 0)
            {
                var list = string.Join("; ", soundtrackCues.Select(c => c.Title).Take(60));
                cuesHint =
                    "For reference, this film's official soundtrack/score track listing is: " +
                    $"[{list}]. Treat this ONLY as a hint: if what you hear clearly matches one of " +
                    "these cues, name that exact title; but this list is NOT exhaustive — if the clip " +
                    "is a needle-drop or any track not on the list, identify it normally, and never pick " +
                    "a listed title just because it is on the list or is the film's famous cue. ";
            }
            var prompt =
                "You are an expert at identifying music by ear. This is a short audio clip taken " +
                $"from the film \"{movieTitle}\"{yearText} (it may have dialogue or sound effects over it). " +
                $"A fingerprint service labelled it \"{currentTitle}\"" +
                (string.IsNullOrWhiteSpace(currentArtist) ? "" : $" by \"{currentArtist}\"") +
                ", which may be wrong. " +
                "Identify the music STRICTLY from what you actually hear in this clip. " +
                "The film name is context only — do NOT assume it is one of that film's famous or " +
                "signature songs, and never invent a specific well-known track title just to fit the film. " +
                cuesHint +
                "Separate two judgements: (a) the exact TITLE — only give one you can genuinely recognise " +
                "by ear, otherwise null; (b) whether it is an instrumental film-score cue and, if so, the " +
                "likely COMPOSER and musical style — you MAY name the composer/style when you can hear it " +
                "is orchestral score even if you cannot name the exact cue, since composer/style is more " +
                "reliable than the exact title. If the music is too faint or buried under dialogue to tell " +
                "anything, return nulls and \"low\" confidence rather than guessing. " +
                "Reply ONLY with a JSON object with these keys: " +
                "\"title\" (track/cue title you actually recognise, or null if unsure), " +
                "\"artist\" (performer or composer, or null if unsure), " +
                "\"is_score_cue\" (true if it's an instrumental film-score cue, false if a commercial/pop song), " +
                "\"confidence\" (\"low\", \"medium\" or \"high\"; use \"low\" whenever you are guessing a title), " +
                "\"explanation\" (one or two sentences; state honestly if you cannot identify it). " +
                "Prefer the composer/artist you are confident about even when unsure of the exact title.";

            var payload = new
            {
                model = AudioModel,
                modalities = new[] { "text" },
                messages = new object[]
                {
                    new
                    {
                        role = "user",
                        content = new object[]
                        {
                            new { type = "text", text = prompt },
                            new
                            {
                                type = "input_audio",
                                input_audio = new { data = audioB64, format = "wav" }
                            }
                        }
                    }
                }
            };

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(120);

            using var req = new HttpRequestMessage(
                HttpMethod.Post, "https://api.openai.com/v1/chat/completions");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
            req.Content = new StringContent(
                JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");

            using var resp = await client.SendAsync(req, cancellationToken);
            var body = await resp.Content.ReadAsStringAsync(cancellationToken);
            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogDebug("Audio model call returned {Status}: {Body}",
                    resp.StatusCode, body);
                return new AudioIdentifySuggestion
                {
                    Error = $"Audio model request failed ({(int)resp.StatusCode})."
                };
            }

            using var doc = JsonDocument.Parse(body);
            var content = doc.RootElement
                .GetProperty("choices")[0]
                .GetProperty("message")
                .GetProperty("content")
                .GetString() ?? "";

            return ParseSuggestion(content);
        }

        // Turns the audio model's composer/style read into a specific soundtrack
        // cue title via a web-search reasoning pass (OpenAI Responses API), then
        // merges any found title/artist into the suggestion. Best-effort: if the
        // model can't find a real match it leaves the title alone.
        private async Task RefineTitleWithWebSearchAsync(
            string apiKey,
            AudioIdentifySuggestion suggestion,
            string currentTitle,
            string? currentArtist,
            string movieTitle,
            int? movieYear,
            IReadOnlyList<SoundtrackCue> soundtrackCues,
            CancellationToken cancellationToken)
        {
            var yearText = movieYear.HasValue ? $" ({movieYear})" : "";
            var heard = new StringBuilder();
            if (!string.IsNullOrWhiteSpace(suggestion.Artist))
                heard.Append($"Performer/composer heard: {suggestion.Artist}. ");
            heard.Append(suggestion.IsScoreCue
                ? "It is an instrumental film-score cue. "
                : "It is a commercial/pop song, not a score cue. ");
            if (!string.IsNullOrWhiteSpace(suggestion.Explanation))
                heard.Append($"Audio notes: {suggestion.Explanation} ");

            var cuesHint = soundtrackCues.Count > 0
                ? "This film's official soundtrack/score track listing is: " +
                  $"[{string.Join("; ", soundtrackCues.Select(c => c.Title).Take(60))}]. Prefer titles from this " +
                  "list when consistent with what was heard, but the list is not exhaustive — you may include a " +
                  "cue not listed if it fits better. "
                : "";
            var prompt =
                $"A short music clip from the film \"{movieTitle}\"{yearText} was identified by ear. {heard}" +
                $"A fingerprint service (often wrong) had labelled it \"{currentTitle}\"" +
                (string.IsNullOrWhiteSpace(currentArtist) ? "" : $" by \"{currentArtist}\"") + ". " +
                cuesHint +
                "Using web search of this film's official soundtrack and score track listing, return a RANKED " +
                "SHORTLIST of up to 5 candidate cue/track titles from that film that are most consistent with " +
                "the specific musical details described above (composer, instrumentation, melody, style), best " +
                "guess first. This is an advisory shortlist a human will listen to and confirm. " +
                "IMPORTANT: do NOT rank a title higher just because it is this film's famous, signature or most " +
                "prominent song — order by how well each fits the described style/scene, not by fame. " +
                "Return an empty list only if nothing on the soundtrack is plausibly consistent with what was heard. " +
                "This is reasoned guessing, not aural recognition, so always report \"low\" confidence. " +
                "Reply ONLY with a JSON object with keys: " +
                "\"candidates\" (an array, best first, of objects with \"title\" and \"artist\" (performer/composer or null)), " +
                "\"confidence\" (always \"low\"), " +
                "\"explanation\" (one sentence on how to tell the top candidates apart).";

            var payload = new
            {
                model = WebSearchModel,
                tools = new[] { new { type = "web_search_preview" } },
                input = prompt
            };

            var client = _httpClientFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(120);
            using var req = new HttpRequestMessage(
                HttpMethod.Post, "https://api.openai.com/v1/responses");
            req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
            req.Content = new StringContent(
                JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");

            using var resp = await client.SendAsync(req, cancellationToken);
            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogDebug("Web-search title call returned {Status}.", resp.StatusCode);
                BuildCandidatesFromSoundtrack(suggestion, soundtrackCues, null);
                return;
            }

            var body = await resp.Content.ReadAsStringAsync(cancellationToken);
            var text = ExtractResponsesText(body);

            var (ranked, explanation) = ParseRankedCandidates(text);
            if (!string.IsNullOrWhiteSpace(explanation))
                suggestion.Explanation = explanation;

            BuildCandidatesFromSoundtrack(suggestion, soundtrackCues, ranked);
        }

        // Populates suggestion.Candidates with a ranked shortlist of real cues,
        // each carrying Spotify + YouTube links so a human can listen and pick.
        // Prefers the model's ranking when available; otherwise falls back to the
        // soundtrack listing order so the user still gets a pick-list with links.
        // The top candidate is also mirrored onto Title (flagged unverified) for
        // backward compatibility with the single-title UI.
        private static void BuildCandidatesFromSoundtrack(
            AudioIdentifySuggestion suggestion,
            IReadOnlyList<SoundtrackCue> soundtrackCues,
            IReadOnlyList<(string Title, string? Artist)>? ranked)
        {
            const int MaxCandidates = 5;
            var byNorm = new Dictionary<string, SoundtrackCue>();
            foreach (var c in soundtrackCues)
            {
                var key = NormalizeTitle(c.Title);
                if (!string.IsNullOrEmpty(key) && !byNorm.ContainsKey(key))
                    byNorm[key] = c;
            }

            var candidates = new List<AudioCueCandidate>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            void Add(string title, string? artist, SoundtrackCue? match)
            {
                if (string.IsNullOrWhiteSpace(title)) return;
                if (!seen.Add(NormalizeTitle(title))) return;
                var finalArtist = !string.IsNullOrWhiteSpace(artist) ? artist : match?.Artist;
                candidates.Add(new AudioCueCandidate
                {
                    Title = title.Trim(),
                    Artist = finalArtist,
                    SpotifyUrl = match?.SpotifyUrl,
                    YouTubeUrl = YouTubeSearchUrl(title, finalArtist)
                });
            }

            if (ranked != null)
            {
                foreach (var r in ranked)
                {
                    if (candidates.Count >= MaxCandidates) break;
                    byNorm.TryGetValue(NormalizeTitle(r.Title), out var match);
                    Add(r.Title, r.Artist, match);
                }
            }

            // Fall back to (or top up with) the soundtrack listing so there is
            // always something to pick from with real listen links.
            foreach (var c in soundtrackCues)
            {
                if (candidates.Count >= MaxCandidates) break;
                Add(c.Title, c.Artist, c);
            }

            if (candidates.Count == 0) return;

            suggestion.Candidates = candidates;
            // Mirror the top pick onto the legacy single-title fields, clearly
            // unverified — the shortlist is the source of truth.
            suggestion.Title = candidates[0].Title;
            suggestion.TitleUnverified = true;
            if (!string.IsNullOrWhiteSpace(candidates[0].Artist))
                suggestion.Artist = candidates[0].Artist;
        }

        private static string YouTubeSearchUrl(string? title, string? artist)
        {
            var query = string.Join(" ", new[] { artist, title }
                .Where(s => !string.IsNullOrWhiteSpace(s))).Trim();
            return "https://www.youtube.com/results?search_query=" + Uri.EscapeDataString(query);
        }

        private static string NormalizeTitle(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return "";
            var lower = s.Trim().ToLowerInvariant();
            // Drop parenthetical qualifiers (e.g. "(From ...)") and punctuation so
            // the model's title and the album track name line up.
            lower = Regex.Replace(lower, @"\([^)]*\)", " ");
            lower = Regex.Replace(lower, @"[^a-z0-9]+", " ");
            return Regex.Replace(lower, @"\s+", " ").Trim();
        }

        // Parses the ranked-candidates JSON: {"candidates":[{"title","artist"}],
        // "explanation"}. Tolerant of the model returning a bare {"title"} object
        // or a plain array. Returns an ordered list (best first) and explanation.
        private static (IReadOnlyList<(string Title, string? Artist)> Ranked, string? Explanation)
            ParseRankedCandidates(string content)
        {
            var ranked = new List<(string, string?)>();
            if (string.IsNullOrWhiteSpace(content))
                return (ranked, null);

            var startObj = content.IndexOf('{');
            var startArr = content.IndexOf('[');
            var start = startObj < 0 ? startArr
                : startArr < 0 ? startObj
                : Math.Min(startObj, startArr);
            if (start < 0) return (ranked, null);
            var endObj = content.LastIndexOf('}');
            var endArr = content.LastIndexOf(']');
            var end = Math.Max(endObj, endArr);
            if (end <= start) return (ranked, null);

            var json = content.Substring(start, end - start + 1);
            var doc = TryParseLenient(json);
            if (doc == null) return (ranked, null);

            string? explanation = null;
            using (doc)
            {
                var root = doc.RootElement;
                JsonElement arr;
                if (root.ValueKind == JsonValueKind.Array)
                {
                    arr = root;
                }
                else if (root.ValueKind == JsonValueKind.Object)
                {
                    explanation = ReadString(root, "explanation");
                    if (!root.TryGetProperty("candidates", out arr)
                        || arr.ValueKind != JsonValueKind.Array)
                    {
                        // Single-object fallback: {"title","artist"}.
                        var t = ReadString(root, "title");
                        if (!string.IsNullOrWhiteSpace(t))
                            ranked.Add((t!, ReadString(root, "artist")));
                        return (ranked, explanation);
                    }
                }
                else
                {
                    return (ranked, null);
                }

                foreach (var el in arr.EnumerateArray())
                {
                    if (el.ValueKind == JsonValueKind.String)
                    {
                        var s = el.GetString();
                        if (!string.IsNullOrWhiteSpace(s)) ranked.Add((s!, null));
                    }
                    else if (el.ValueKind == JsonValueKind.Object)
                    {
                        var t = ReadString(el, "title");
                        if (!string.IsNullOrWhiteSpace(t))
                            ranked.Add((t!, ReadString(el, "artist")));
                    }
                }
            }

            return (ranked, explanation);
        }

        // Concatenates the "output_text" parts of an OpenAI Responses API result.
        private static string ExtractResponsesText(string body)
        {
            var text = new StringBuilder();
            using var doc = JsonDocument.Parse(body);
            if (!doc.RootElement.TryGetProperty("output", out var output)
                || output.ValueKind != JsonValueKind.Array)
                return "";
            foreach (var item in output.EnumerateArray())
            {
                if (!item.TryGetProperty("content", out var content)
                    || content.ValueKind != JsonValueKind.Array)
                    continue;
                foreach (var c in content.EnumerateArray())
                {
                    if (c.TryGetProperty("type", out var ct) && ct.GetString() == "output_text"
                        && c.TryGetProperty("text", out var tx) && tx.ValueKind == JsonValueKind.String)
                        text.Append(tx.GetString());
                }
            }
            return text.ToString();
        }

        private static AudioIdentifySuggestion ParseSuggestion(string content)
        {
            var suggestion = new AudioIdentifySuggestion { Raw = content };

            var startIdx = content.IndexOf('{');
            var endIdx = content.LastIndexOf('}');
            if (startIdx < 0 || endIdx <= startIdx)
            {
                suggestion.Explanation = content.Trim();
                return suggestion;
            }

            var json = content.Substring(startIdx, endIdx - startIdx + 1);
            var doc = TryParseLenient(json);
            if (doc == null)
            {
                suggestion.Explanation = content.Trim();
                return suggestion;
            }

            using (doc)
            {
                var root = doc.RootElement;
                suggestion.Title = ReadString(root, "title");
                suggestion.Artist = ReadString(root, "artist");
                suggestion.Confidence = ReadString(root, "confidence");
                suggestion.Explanation = ReadString(root, "explanation");
                if (root.TryGetProperty("is_score_cue", out var sc))
                {
                    suggestion.IsScoreCue = sc.ValueKind == JsonValueKind.True
                        || (sc.ValueKind == JsonValueKind.String
                            && bool.TryParse(sc.GetString(), out var b) && b);
                }
            }

            return suggestion;
        }

        // Parse the model's JSON, tolerating a common malformation: emitting a
        // bare (unquoted) enum value, e.g. `"confidence": medium`. Quotes those
        // and retries so a stray formatting slip doesn't discard an otherwise
        // usable suggestion. Returns null only when it's still unparseable.
        private static JsonDocument? TryParseLenient(string json)
        {
            var options = new JsonDocumentOptions { AllowTrailingCommas = true };
            try
            {
                return JsonDocument.Parse(json, options);
            }
            catch (JsonException)
            {
                var repaired = Regex.Replace(
                    json,
                    "(\"(?:confidence|title|artist)\"\\s*:\\s*)(low|medium|high)\\b",
                    "$1\"$2\"",
                    RegexOptions.IgnoreCase);
                try
                {
                    return JsonDocument.Parse(repaired, options);
                }
                catch (JsonException)
                {
                    return null;
                }
            }
        }

        private static string? ReadString(JsonElement root, string name)
        {
            if (!root.TryGetProperty(name, out var el)) return null;
            if (el.ValueKind == JsonValueKind.Null) return null;
            var s = el.ValueKind == JsonValueKind.String ? el.GetString() : el.ToString();
            if (string.IsNullOrWhiteSpace(s)) return null;
            var trimmed = s.Trim();
            return trimmed.Equals("null", StringComparison.OrdinalIgnoreCase) ? null : trimmed;
        }
    }
}
