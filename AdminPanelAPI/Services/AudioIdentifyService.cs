using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
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

            try
            {
                return await AskAudioModelAsync(
                    apiKey, audioB64, currentTitle, currentArtist,
                    movieTitle, movieYear, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Audio identification call failed.");
                return new AudioIdentifySuggestion { Error = "The audio model call failed." };
            }
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
            CancellationToken cancellationToken)
        {
            var yearText = movieYear.HasValue ? $" ({movieYear})" : "";
            var prompt =
                "You are an expert at identifying music by ear. This is a short audio clip taken " +
                $"from the film \"{movieTitle}\"{yearText} (it may have dialogue or sound effects over it). " +
                $"A fingerprint service labelled it \"{currentTitle}\"" +
                (string.IsNullOrWhiteSpace(currentArtist) ? "" : $" by \"{currentArtist}\"") +
                ", which may be wrong. " +
                "Identify the music STRICTLY from what you actually hear in this clip. " +
                "The film name is context only — do NOT assume it is one of that film's famous or " +
                "signature songs, and never invent a specific well-known track title just to fit the film. " +
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

            try
            {
                using var doc = JsonDocument.Parse(content.Substring(startIdx, endIdx - startIdx + 1));
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
            catch (JsonException)
            {
                suggestion.Explanation = content.Trim();
            }

            return suggestion;
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
