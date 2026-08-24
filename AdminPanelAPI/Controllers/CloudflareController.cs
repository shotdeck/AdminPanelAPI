using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/cloudflare")]
    public sealed class CloudflareController : ControllerBase
    {
        private const string GraphqlEndpoint = "https://api.cloudflare.com/client/v4/graphql";

        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IConfiguration _config;
        private readonly ILogger<CloudflareController> _logger;

        private static readonly JsonSerializerOptions JsonOpts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };

        public CloudflareController(
            IHttpClientFactory httpClientFactory,
            IConfiguration config,
            ILogger<CloudflareController> logger)
        {
            _httpClientFactory = httpClientFactory;
            _config = config;
            _logger = logger;
        }

        private (string token, string zoneId) GetCredentials()
        {
            var token = _config["CLOUDFLARE:CF_API_TOKEN"]
                ?? throw new InvalidOperationException("CF_API_TOKEN is not configured.");
            var zoneId = _config["CLOUDFLARE:CF_ZONE_ID"];
            return (token, zoneId);
        }

        private (string since, string until) DefaultDates(string? since, string? until)
        {
            var end = until ?? DateTime.UtcNow.ToString("yyyy-MM-dd");
            var start = since ?? DateTime.UtcNow.AddDays(-30).ToString("yyyy-MM-dd");
            return (start, end);
        }

        private async Task<JsonElement> ExecuteGraphqlAsync(
            string query, Dictionary<string, object> variables, CancellationToken ct)
        {
            var (token, _) = GetCredentials();
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var payload = new { query, variables };
            var content = new StringContent(
                JsonSerializer.Serialize(payload),
                Encoding.UTF8,
                "application/json");

            var response = await client.PostAsync(GraphqlEndpoint, content, ct);
            var body = await response.Content.ReadAsStringAsync(ct);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("Cloudflare API error {Status}: {Body}", response.StatusCode, body);
                throw new HttpRequestException($"Cloudflare API returned {response.StatusCode}");
            }

            var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            if (root.TryGetProperty("errors", out var errors)
                && errors.ValueKind == JsonValueKind.Array
                && errors.GetArrayLength() > 0)
            {
                var msg = errors[0].GetProperty("message").GetString();
                _logger.LogError("Cloudflare GraphQL error: {Message}", msg);
                throw new HttpRequestException($"Cloudflare GraphQL error: {msg}");
            }

            return root.GetProperty("data");
        }

        private JsonElement GetGroups(JsonElement data)
        {
            return data
                .GetProperty("viewer")
                .GetProperty("zones")[0]
                .GetProperty("httpRequests1dGroups");
        }

        // ── GET /api/admin/cloudflare/health ────────────────────────────
        [HttpGet("health")]
        [ProducesResponseType(Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public ActionResult Health()
        {
            var hasToken = !string.IsNullOrEmpty(_config["CLOUDFLARE:CF_API_TOKEN"]);
            return Ok(new { status = "ok", hasToken });
        }

        [HttpGet("healthtest")]
        [ProducesResponseType(Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public ActionResult HealthTest()
        {
            var hasToken = !string.IsNullOrEmpty(_config["CLOUDFLARE:CF_API_TOKEN"]);
            return Ok(new { status = "ok", hasToken });
        }

        // ── GET /api/admin/cloudflare/overview ──────────────────────────
        [HttpGet("overview")]
        [ProducesResponseType(typeof(OverviewResponse), Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public async Task<ActionResult<OverviewResponse>> Overview(
            [FromQuery] string? since,
            [FromQuery] string? until,
            CancellationToken ct)
        {
            var (s, u) = DefaultDates(since, until);
            var (_, zoneId) = GetCredentials();

            var query = @"
                query Overview($zoneTag: string!, $since: Date!, $until: Date!) {
                  viewer {
                    zones(filter: {zoneTag: $zoneTag}) {
                      httpRequests1dGroups(
                        filter: {date_geq: $since, date_leq: $until}
                        limit: 1000
                        orderBy: [date_ASC]
                      ) {
                        dimensions { date }
                        sum {
                          requests bytes cachedRequests cachedBytes
                          threats pageViews encryptedRequests encryptedBytes
                        }
                        uniq { uniques }
                      }
                    }
                  }
                }";

            var variables = new Dictionary<string, object>
            {
                ["zoneTag"] = zoneId, ["since"] = s, ["until"] = u
            };

            var data = await ExecuteGraphqlAsync(query, variables, ct);
            var groups = GetGroups(data);

            var totals = new OverviewTotals();
            var timeseries = new List<TimeseriesPoint>();

            foreach (var g in groups.EnumerateArray())
            {
                var sum = g.GetProperty("sum");
                var uniq = g.GetProperty("uniq");
                var date = g.GetProperty("dimensions").GetProperty("date").GetString()!;

                var requests = sum.GetProperty("requests").GetInt64();
                var bytes = sum.GetProperty("bytes").GetInt64();
                var cachedRequests = sum.GetProperty("cachedRequests").GetInt64();
                var cachedBytes = sum.GetProperty("cachedBytes").GetInt64();
                var threats = sum.GetProperty("threats").GetInt64();
                var pageViews = sum.GetProperty("pageViews").GetInt64();
                var encryptedRequests = sum.GetProperty("encryptedRequests").GetInt64();
                var encryptedBytes = sum.GetProperty("encryptedBytes").GetInt64();
                var uniques = uniq.GetProperty("uniques").GetInt64();

                totals.Requests += requests;
                totals.Bandwidth += bytes;
                totals.CachedRequests += cachedRequests;
                totals.CachedBytes += cachedBytes;
                totals.Threats += threats;
                totals.PageViews += pageViews;
                totals.EncryptedRequests += encryptedRequests;
                totals.EncryptedBytes += encryptedBytes;
                totals.UniqueVisitors += uniques;

                timeseries.Add(new TimeseriesPoint
                {
                    Date = date,
                    Requests = requests,
                    Bandwidth = bytes,
                    CachedRequests = cachedRequests,
                    CachedBytes = cachedBytes,
                    Threats = threats,
                    PageViews = pageViews,
                    UniqueVisitors = uniques,
                    EncryptedRequests = encryptedRequests,
                    EncryptedBytes = encryptedBytes,
                });
            }

            return Ok(new OverviewResponse { Totals = totals, Timeseries = timeseries });
        }

        // ── GET /api/admin/cloudflare/countries ─────────────────────────
        [HttpGet("countries")]
        [ProducesResponseType(typeof(CountriesResponse), Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public async Task<ActionResult<CountriesResponse>> Countries(
            [FromQuery] string? since,
            [FromQuery] string? until,
            CancellationToken ct)
        {
            var (s, u) = DefaultDates(since, until);
            var (_, zoneId) = GetCredentials();

            var query = @"
                query Countries($zoneTag: string!, $since: Date!, $until: Date!) {
                  viewer {
                    zones(filter: {zoneTag: $zoneTag}) {
                      httpRequests1dGroups(
                        filter: {date_geq: $since, date_leq: $until}
                        limit: 1000
                      ) {
                        sum {
                          countryMap {
                            clientCountryName requests bytes threats
                          }
                        }
                      }
                    }
                  }
                }";

            var variables = new Dictionary<string, object>
            {
                ["zoneTag"] = zoneId, ["since"] = s, ["until"] = u
            };

            var data = await ExecuteGraphqlAsync(query, variables, ct);
            var groups = GetGroups(data);

            var agg = new Dictionary<string, CountryEntry>();
            foreach (var g in groups.EnumerateArray())
            {
                foreach (var c in g.GetProperty("sum").GetProperty("countryMap").EnumerateArray())
                {
                    var name = c.GetProperty("clientCountryName").GetString()!;
                    if (!agg.TryGetValue(name, out var entry))
                    {
                        entry = new CountryEntry { Country = name };
                        agg[name] = entry;
                    }
                    entry.Requests += c.GetProperty("requests").GetInt64();
                    entry.Bandwidth += c.GetProperty("bytes").GetInt64();
                    entry.Threats += c.GetProperty("threats").GetInt64();
                }
            }

            var result = agg.Values.OrderByDescending(x => x.Requests).ToList();
            return Ok(new CountriesResponse { Countries = result });
        }

        // ── GET /api/admin/cloudflare/content-types ─────────────────────
        [HttpGet("content-types")]
        [ProducesResponseType(typeof(ContentTypesResponse), Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public async Task<ActionResult<ContentTypesResponse>> ContentTypes(
            [FromQuery] string? since,
            [FromQuery] string? until,
            CancellationToken ct)
        {
            var (s, u) = DefaultDates(since, until);
            var (_, zoneId) = GetCredentials();

            var query = @"
                query ContentTypes($zoneTag: string!, $since: Date!, $until: Date!) {
                  viewer {
                    zones(filter: {zoneTag: $zoneTag}) {
                      httpRequests1dGroups(
                        filter: {date_geq: $since, date_leq: $until}
                        limit: 1000
                      ) {
                        sum {
                          contentTypeMap {
                            edgeResponseContentTypeName requests bytes
                          }
                        }
                      }
                    }
                  }
                }";

            var variables = new Dictionary<string, object>
            {
                ["zoneTag"] = zoneId, ["since"] = s, ["until"] = u
            };

            var data = await ExecuteGraphqlAsync(query, variables, ct);
            var groups = GetGroups(data);

            var agg = new Dictionary<string, ContentTypeEntry>();
            foreach (var g in groups.EnumerateArray())
            {
                foreach (var c in g.GetProperty("sum").GetProperty("contentTypeMap").EnumerateArray())
                {
                    var name = c.GetProperty("edgeResponseContentTypeName").GetString()!;
                    if (!agg.TryGetValue(name, out var entry))
                    {
                        entry = new ContentTypeEntry { ContentType = name };
                        agg[name] = entry;
                    }
                    entry.Requests += c.GetProperty("requests").GetInt64();
                    entry.Bandwidth += c.GetProperty("bytes").GetInt64();
                }
            }

            var result = agg.Values.OrderByDescending(x => x.Requests).ToList();
            return Ok(new ContentTypesResponse { ContentTypes = result });
        }

        // ── GET /api/admin/cloudflare/status-codes ──────────────────────
        [HttpGet("status-codes")]
        [ProducesResponseType(typeof(StatusCodesResponse), Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public async Task<ActionResult<StatusCodesResponse>> StatusCodes(
            [FromQuery] string? since,
            [FromQuery] string? until,
            CancellationToken ct)
        {
            var (s, u) = DefaultDates(since, until);
            var (_, zoneId) = GetCredentials();

            var query = @"
                query StatusCodes($zoneTag: string!, $since: Date!, $until: Date!) {
                  viewer {
                    zones(filter: {zoneTag: $zoneTag}) {
                      httpRequests1dGroups(
                        filter: {date_geq: $since, date_leq: $until}
                        limit: 1000
                      ) {
                        sum {
                          responseStatusMap {
                            edgeResponseStatus requests
                          }
                        }
                      }
                    }
                  }
                }";

            var variables = new Dictionary<string, object>
            {
                ["zoneTag"] = zoneId, ["since"] = s, ["until"] = u
            };

            var data = await ExecuteGraphqlAsync(query, variables, ct);
            var groups = GetGroups(data);

            var agg = new Dictionary<int, long>();
            foreach (var g in groups.EnumerateArray())
            {
                foreach (var c in g.GetProperty("sum").GetProperty("responseStatusMap").EnumerateArray())
                {
                    var code = c.GetProperty("edgeResponseStatus").GetInt32();
                    agg.TryGetValue(code, out var existing);
                    agg[code] = existing + c.GetProperty("requests").GetInt64();
                }
            }

            var result = agg
                .OrderBy(x => x.Key)
                .Select(x => new StatusCodeEntry { Status = x.Key, Requests = x.Value })
                .ToList();
            return Ok(new StatusCodesResponse { StatusCodes = result });
        }

        // ── GET /api/admin/cloudflare/browsers ──────────────────────────
        [HttpGet("browsers")]
        [ProducesResponseType(typeof(BrowsersResponse), Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public async Task<ActionResult<BrowsersResponse>> Browsers(
            [FromQuery] string? since,
            [FromQuery] string? until,
            CancellationToken ct)
        {
            var (s, u) = DefaultDates(since, until);
            var (_, zoneId) = GetCredentials();

            var query = @"
                query Browsers($zoneTag: string!, $since: Date!, $until: Date!) {
                  viewer {
                    zones(filter: {zoneTag: $zoneTag}) {
                      httpRequests1dGroups(
                        filter: {date_geq: $since, date_leq: $until}
                        limit: 1000
                      ) {
                        sum {
                          browserMap {
                            uaBrowserFamily pageViews
                          }
                        }
                      }
                    }
                  }
                }";

            var variables = new Dictionary<string, object>
            {
                ["zoneTag"] = zoneId, ["since"] = s, ["until"] = u
            };

            var data = await ExecuteGraphqlAsync(query, variables, ct);
            var groups = GetGroups(data);

            var agg = new Dictionary<string, long>();
            foreach (var g in groups.EnumerateArray())
            {
                foreach (var c in g.GetProperty("sum").GetProperty("browserMap").EnumerateArray())
                {
                    var name = c.GetProperty("uaBrowserFamily").GetString()!;
                    agg.TryGetValue(name, out var existing);
                    agg[name] = existing + c.GetProperty("pageViews").GetInt64();
                }
            }

            var result = agg
                .OrderByDescending(x => x.Value)
                .Select(x => new BrowserEntry { Browser = x.Key, PageViews = x.Value })
                .ToList();
            return Ok(new BrowsersResponse { Browsers = result });
        }

        // ── GET /api/admin/cloudflare/ssl ───────────────────────────────
        [HttpGet("ssl")]
        [ProducesResponseType(typeof(SslResponse), Microsoft.AspNetCore.Http.StatusCodes.Status200OK)]
        public async Task<ActionResult<SslResponse>> Ssl(
            [FromQuery] string? since,
            [FromQuery] string? until,
            CancellationToken ct)
        {
            var (s, u) = DefaultDates(since, until);
            var (_, zoneId) = GetCredentials();

            var query = @"
                query Ssl($zoneTag: string!, $since: Date!, $until: Date!) {
                  viewer {
                    zones(filter: {zoneTag: $zoneTag}) {
                      httpRequests1dGroups(
                        filter: {date_geq: $since, date_leq: $until}
                        limit: 1000
                      ) {
                        sum {
                          clientSSLMap {
                            clientSSLProtocol requests
                          }
                        }
                      }
                    }
                  }
                }";

            var variables = new Dictionary<string, object>
            {
                ["zoneTag"] = zoneId, ["since"] = s, ["until"] = u
            };

            var data = await ExecuteGraphqlAsync(query, variables, ct);
            var groups = GetGroups(data);

            var agg = new Dictionary<string, long>();
            foreach (var g in groups.EnumerateArray())
            {
                foreach (var c in g.GetProperty("sum").GetProperty("clientSSLMap").EnumerateArray())
                {
                    var name = c.GetProperty("clientSSLProtocol").GetString()!;
                    agg.TryGetValue(name, out var existing);
                    agg[name] = existing + c.GetProperty("requests").GetInt64();
                }
            }

            var result = agg
                .OrderByDescending(x => x.Value)
                .Select(x => new SslEntry { Protocol = x.Key, Requests = x.Value })
                .ToList();
            return Ok(new SslResponse { Ssl = result });
        }

        // ── DTOs ────────────────────────────────────────────────────────

        public sealed class OverviewResponse
        {
            public OverviewTotals Totals { get; set; } = new();
            public List<TimeseriesPoint> Timeseries { get; set; } = new();
        }

        public sealed class OverviewTotals
        {
            public long Requests { get; set; }
            public long Bandwidth { get; set; }
            public long CachedRequests { get; set; }
            public long CachedBytes { get; set; }
            public long Threats { get; set; }
            public long PageViews { get; set; }
            public long UniqueVisitors { get; set; }
            public long EncryptedRequests { get; set; }
            public long EncryptedBytes { get; set; }
        }

        public sealed class TimeseriesPoint
        {
            public string Date { get; set; } = "";
            public long Requests { get; set; }
            public long Bandwidth { get; set; }
            public long CachedRequests { get; set; }
            public long CachedBytes { get; set; }
            public long Threats { get; set; }
            public long PageViews { get; set; }
            public long UniqueVisitors { get; set; }
            public long EncryptedRequests { get; set; }
            public long EncryptedBytes { get; set; }
        }

        public sealed class CountriesResponse
        {
            public List<CountryEntry> Countries { get; set; } = new();
        }

        public sealed class CountryEntry
        {
            public string Country { get; set; } = "";
            public long Requests { get; set; }
            public long Bandwidth { get; set; }
            public long Threats { get; set; }
        }

        public sealed class ContentTypesResponse
        {
            public List<ContentTypeEntry> ContentTypes { get; set; } = new();
        }

        public sealed class ContentTypeEntry
        {
            public string ContentType { get; set; } = "";
            public long Requests { get; set; }
            public long Bandwidth { get; set; }
        }

        public sealed class StatusCodesResponse
        {
            public List<StatusCodeEntry> StatusCodes { get; set; } = new();
        }

        public sealed class StatusCodeEntry
        {
            public int Status { get; set; }
            public long Requests { get; set; }
        }

        public sealed class BrowsersResponse
        {
            public List<BrowserEntry> Browsers { get; set; } = new();
        }

        public sealed class BrowserEntry
        {
            public string Browser { get; set; } = "";
            public long PageViews { get; set; }
        }

        public sealed class SslResponse
        {
            public List<SslEntry> Ssl { get; set; } = new();
        }

        public sealed class SslEntry
        {
            public string Protocol { get; set; } = "";
            public long Requests { get; set; }
        }
    }
}
