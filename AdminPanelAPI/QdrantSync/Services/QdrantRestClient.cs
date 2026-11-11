using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>The Qdrant REST calls the sync, insert and rebuild jobs need.</summary>
public sealed class QdrantRestClient(HttpClient http, ILogger logger)
{
    private const int MaxAttempts = 5;

    /// <summary>Payloads of whichever of <paramref name="ids"/> exist in the collection.</summary>
    public async Task<Dictionary<long, Dictionary<string, object?>>> GetPayloadsAsync(
        string collection, IReadOnlyCollection<int> ids, CancellationToken ct)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new { ids, with_payload = true, with_vector = false });
        using var response = await PostAsync($"collections/{Uri.EscapeDataString(collection)}/points", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant points read returned unexpected status");
        var found = new Dictionary<long, Dictionary<string, object?>>();
        foreach (var point in doc.RootElement.GetProperty("result").EnumerateArray())
        {
            var payload = point.GetProperty("payload");
            found[point.GetProperty("id").GetInt64()] = payload.ValueKind == JsonValueKind.Object
                ? JsonSerializer.Deserialize<Dictionary<string, object?>>(payload.GetRawText())!
                : new();
        }
        return found;
    }

    /// <summary>One batch request with a <c>set_payload</c> per point.</summary>
    public async Task SetPayloadsAsync(
        string collection, IEnumerable<(long Id, Dictionary<string, object?> Payload)> items, CancellationToken ct)
    {
        var operations = items.Select(x => new { set_payload = new { points = new[] { x.Id }, payload = x.Payload } }).ToArray();
        var body = JsonSerializer.SerializeToUtf8Bytes(new { operations });
        using var response = await PostAsync(
            $"collections/{Uri.EscapeDataString(collection)}/points/batch?wait=true&ordering=weak", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant batch write returned unexpected status");
    }

    /// <summary>Every point id in the collection (no payloads or vectors), paged with scroll.</summary>
    public async Task<HashSet<long>> ScrollAllIdsAsync(string collection, int pageSize, CancellationToken ct)
    {
        var ids = new HashSet<long>();
        long? offset = null;
        var url = $"collections/{Uri.EscapeDataString(collection)}/points/scroll";
        while (true)
        {
            var body = JsonSerializer.SerializeToUtf8Bytes(new { limit = pageSize, offset, with_payload = false, with_vector = false });
            using var response = await PostAsync(url, body, ct);
            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
            if (doc.RootElement.GetProperty("status").GetString() != "ok")
                throw new IOException("Qdrant scroll returned unexpected status");
            var result = doc.RootElement.GetProperty("result");
            foreach (var point in result.GetProperty("points").EnumerateArray())
                ids.Add(point.GetProperty("id").GetInt64());
            var next = result.GetProperty("next_page_offset");
            if (next.ValueKind == JsonValueKind.Null) return ids;
            var nextOffset = next.GetInt64();
            if (offset.HasValue && nextOffset <= offset.Value)
                throw new IOException($"Qdrant scroll did not advance beyond {offset.Value}.");
            offset = nextOffset;
        }
    }

    /// <summary>Which of <paramref name="ids"/> exist.</summary>
    public async Task<HashSet<long>> GetExistingIdsAsync(string collection, IReadOnlyCollection<long> ids, CancellationToken ct)
    {
        var found = new HashSet<long>();
        if (ids.Count == 0) return found;
        var body = JsonSerializer.SerializeToUtf8Bytes(new { ids, with_payload = false, with_vector = false });
        using var response = await PostAsync($"collections/{Uri.EscapeDataString(collection)}/points", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant existence lookup returned unexpected status");
        foreach (var point in doc.RootElement.GetProperty("result").EnumerateArray())
            found.Add(point.GetProperty("id").GetInt64());
        return found;
    }

    /// <summary>Upserts points with their qwen3 vector, their cinemanet vector when they have one, and payload.</summary>
    public async Task UpsertPointsAsync(string collection, string vectorName, IEnumerable<ImagePoint> points, CancellationToken ct,
        string? cinemanetVectorName = null)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            points = points.Select(p =>
            {
                var vector = new Dictionary<string, float[]> { [vectorName] = p.Vector ?? throw new InvalidOperationException($"Image {p.Id} has no vector.") };
                if (cinemanetVectorName != null && p.Cinemanet != null) vector[cinemanetVectorName] = p.Cinemanet;
                return new { id = p.Id, vector, payload = p.Payload };
            })
        });
        await PutPointsAsync(collection, body, ct);
    }

    /// <summary>Upserts points copied from another collection: raw payload plus every named vector given.</summary>
    public async Task UpsertCopiedPointsAsync(string collection, IEnumerable<CopiedPoint> points, CancellationToken ct)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            points = points.Select(p => new { id = p.Id, vector = p.Vectors, payload = p.Payload })
        });
        await PutPointsAsync(collection, body, ct);
    }

    private async Task PutPointsAsync(string collection, byte[] body, CancellationToken ct)
    {
        using var response = await SendAsync(HttpMethod.Put,
            $"collections/{Uri.EscapeDataString(collection)}/points?wait=true&ordering=weak", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant upsert returned unexpected status");
    }

    /// <summary>
    /// Sets one named vector on existing points, leaving their other vectors and payload alone.
    /// Qdrant rejects the whole request if any id doesn't exist.
    /// </summary>
    public async Task UpdateVectorsAsync(string collection, string vectorName, IEnumerable<(long Id, float[] Vector)> items, CancellationToken ct)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            points = items.Select(x => new { id = x.Id, vector = new Dictionary<string, float[]> { [vectorName] = x.Vector } })
        });
        using var response = await SendAsync(HttpMethod.Put,
            $"collections/{Uri.EscapeDataString(collection)}/points/vectors?wait=true&ordering=weak", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant vector update returned unexpected status");
    }

    /// <summary>Which of <paramref name="ids"/> exist but have no <paramref name="vectorName"/> vector.</summary>
    public async Task<HashSet<long>> IdsWithoutVectorAsync(string collection, string vectorName, IReadOnlyCollection<long> ids, CancellationToken ct)
    {
        var found = new HashSet<long>();
        if (ids.Count == 0) return found;
        var body = JsonSerializer.SerializeToUtf8Bytes(new
        {
            filter = new
            {
                must = new object[] { new { has_id = ids } },
                must_not = new object[] { new { has_vector = vectorName } },
            },
            limit = ids.Count,
            with_payload = false,
            with_vector = false,
        });
        using var response = await PostAsync($"collections/{Uri.EscapeDataString(collection)}/points/scroll", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant scroll returned unexpected status");
        foreach (var point in doc.RootElement.GetProperty("result").GetProperty("points").EnumerateArray())
            found.Add(point.GetProperty("id").GetInt64());
        return found;
    }

    /// <summary>One scroll page with payloads and all vectors, starting at id <paramref name="offset"/> (inclusive).</summary>
    public async Task<(List<CopiedPoint> Points, long? Next)> ScrollPageAsync(string collection, long? offset, int limit, bool withData, CancellationToken ct)
    {
        var body = JsonSerializer.SerializeToUtf8Bytes(new { limit, offset, with_payload = withData, with_vector = withData });
        using var response = await PostAsync($"collections/{Uri.EscapeDataString(collection)}/points/scroll", body, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        if (doc.RootElement.GetProperty("status").GetString() != "ok")
            throw new IOException("Qdrant scroll returned unexpected status");
        var result = doc.RootElement.GetProperty("result");
        var points = new List<CopiedPoint>();
        foreach (var point in result.GetProperty("points").EnumerateArray())
        {
            var vectors = new Dictionary<string, float[]>();
            if (point.TryGetProperty("vector", out var v) && v.ValueKind == JsonValueKind.Object)
                foreach (var named in v.EnumerateObject())
                    vectors[named.Name] = named.Value.Deserialize<float[]>()!;
            var payload = point.TryGetProperty("payload", out var pl) && pl.ValueKind == JsonValueKind.Object ? pl.Clone() : (JsonElement?)null;
            points.Add(new CopiedPoint(point.GetProperty("id").GetInt64(), vectors, payload));
        }
        var next = result.GetProperty("next_page_offset");
        long? nextOffset = next.ValueKind == JsonValueKind.Null ? null : next.GetInt64();
        if (nextOffset.HasValue && offset.HasValue && nextOffset.Value <= offset.Value)
            throw new IOException($"Qdrant scroll did not advance beyond {offset.Value}.");
        return (points, nextOffset);
    }

    /// <summary>The collection's info (<c>config</c>, <c>payload_schema</c>, counts), or null if it doesn't exist.</summary>
    public async Task<JsonElement?> GetCollectionAsync(string collection, CancellationToken ct)
    {
        using (var exists = await SendAsync(HttpMethod.Get, $"collections/{Uri.EscapeDataString(collection)}/exists", null, ct))
        using (var doc = JsonDocument.Parse(await exists.Content.ReadAsStringAsync(ct)))
            if (!doc.RootElement.GetProperty("result").GetProperty("exists").GetBoolean()) return null;
        using var response = await SendAsync(HttpMethod.Get, $"collections/{Uri.EscapeDataString(collection)}", null, ct);
        using var info = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        return info.RootElement.GetProperty("result").Clone();
    }

    /// <summary>Named dense vectors defined on the collection (empty if it doesn't exist).</summary>
    public async Task<HashSet<string>> VectorNamesAsync(string collection, CancellationToken ct) =>
        await GetCollectionAsync(collection, ct) is { } info ? QdrantCollectionConfig.VectorNames(info) : new HashSet<string>();

    /// <summary>The collection an alias points at, or null if <paramref name="alias"/> isn't an alias.</summary>
    public async Task<string?> AliasTargetAsync(string alias, CancellationToken ct)
    {
        using var response = await SendAsync(HttpMethod.Get, "aliases", null, ct);
        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        foreach (var a in doc.RootElement.GetProperty("result").GetProperty("aliases").EnumerateArray())
            if (a.GetProperty("alias_name").GetString() == alias) return a.GetProperty("collection_name").GetString();
        return null;
    }

    public async Task CreateCollectionAsync(string collection, object config, CancellationToken ct)
    {
        using var _ = await SendAsync(HttpMethod.Put, $"collections/{Uri.EscapeDataString(collection)}",
            JsonSerializer.SerializeToUtf8Bytes(config), ct);
    }

    public async Task UpdateCollectionAsync(string collection, object config, CancellationToken ct)
    {
        using var _ = await SendAsync(HttpMethod.Patch, $"collections/{Uri.EscapeDataString(collection)}",
            JsonSerializer.SerializeToUtf8Bytes(config), ct);
    }

    public async Task CreatePayloadIndexAsync(string collection, string field, JsonElement schema, CancellationToken ct, bool wait = true)
    {
        using var _ = await SendAsync(HttpMethod.Put, $"collections/{Uri.EscapeDataString(collection)}/index?wait={(wait ? "true" : "false")}",
            JsonSerializer.SerializeToUtf8Bytes(new { field_name = field, field_schema = schema }), ct);
    }

    private Task<HttpResponseMessage> PostAsync(string url, byte[] body, CancellationToken ct) =>
        SendAsync(HttpMethod.Post, url, body, ct);

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, string url, byte[]? body, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var request = new HttpRequestMessage(method, url);
                if (body != null)
                {
                    request.Content = new ByteArrayContent(body);
                    request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                }
                var response = await http.SendAsync(request, ct);
                if (response.IsSuccessStatusCode) return response;
                var detail = await response.Content.ReadAsStringAsync(ct);
                var retryable = response.StatusCode == HttpStatusCode.TooManyRequests || (int)response.StatusCode >= 500;
                response.Dispose();
                if (!retryable || attempt >= MaxAttempts)
                    throw new IOException($"Qdrant HTTP {(int)response.StatusCode}: {detail}");
                logger.LogWarning("Qdrant {Url} returned {Status}, retry {Attempt}.", url, (int)response.StatusCode, attempt);
            }
            catch (Exception ex) when (attempt < MaxAttempts && !ct.IsCancellationRequested
                                       && ex is HttpRequestException or TaskCanceledException)
            {
                logger.LogWarning("Qdrant {Url} failed ({Message}), retry {Attempt}.", url, ex.Message, attempt);
            }
            await Task.Delay(TimeSpan.FromSeconds(Math.Min(15, Math.Pow(2, attempt))), ct);
        }
    }
}

/// <summary>A point read from one collection to be written to another.</summary>
public sealed record CopiedPoint(long Id, Dictionary<string, float[]> Vectors, JsonElement? Payload);
