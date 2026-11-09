using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>The few Qdrant REST calls the sync needs. Payload-only: vectors are never read or written.</summary>
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

    private async Task<HttpResponseMessage> PostAsync(string url, byte[] body, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var content = new ByteArrayContent(body);
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                var response = await http.PostAsync(url, content, ct);
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
