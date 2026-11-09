using System.Net;
using System.Text;
using System.Text.Json;
using AdminPanelAPI.QdrantSync.Models;
using AdminPanelAPI.QdrantSync.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace AdminPanelAPI.Tests;

public class QdrantSyncTests
{
    private static Dictionary<string, object?> FromJson(string json) =>
        JsonSerializer.Deserialize<Dictionary<string, object?>>(json)!;

    private static Dictionary<string, object?> Source(Dictionary<string, object?> image, Dictionary<string, object?> movie) =>
        new() { ["image"] = image, ["movie"] = movie };

    [Fact]
    public void Reconcile_ReturnsNull_WhenManagedFieldsMatch()
    {
        var existing = FromJson("""{"image":{"movieid":5,"tags":["a","b"],"caption":"keep"},"movie":{"title":"T","year":2001}}""");
        var source = Source(new() { ["movieid"] = 5L, ["tags"] = new[] { "a", "b" } }, new() { ["title"] = "T", ["year"] = 2001 });

        Assert.Null(QdrantPayloads.Reconcile(existing, source, false));
        Assert.Empty(QdrantPayloads.Differences(existing, source, false));
    }

    [Fact]
    public void Reconcile_UpdatesAndRemovesManagedKeys_KeepsUnownedKeys()
    {
        var existing = FromJson("""{"image":{"movieid":5,"lens":"old","caption":"keep"},"movie":{"title":"Old"},"extra":{"x":1}}""");
        var source = Source(new() { ["movieid"] = 5L }, new() { ["title"] = "New" });

        var updated = QdrantPayloads.Reconcile(existing, source, false)!;
        var json = JsonSerializer.Serialize(updated);
        using var doc = JsonDocument.Parse(json);
        var image = doc.RootElement.GetProperty("image");
        Assert.False(image.TryGetProperty("lens", out _));
        Assert.Equal("keep", image.GetProperty("caption").GetString());
        Assert.Equal("New", doc.RootElement.GetProperty("movie").GetProperty("title").GetString());
        Assert.Equal(1, doc.RootElement.GetProperty("extra").GetProperty("x").GetInt32());

        var diffs = QdrantPayloads.Differences(existing, source, false);
        Assert.Contains(diffs, d => d is { Field: "image.lens", Old: "\"old\"", New: "<missing>" });
        Assert.Contains(diffs, d => d is { Field: "movie.title", Old: "\"Old\"", New: "\"New\"" });
        Assert.Equal(2, diffs.Count);
    }

    [Fact]
    public void Reconcile_IgnoresRgbColors_UnlessColorShadesIncluded()
    {
        var existing = FromJson("""{"image":{"rgb_colors":["#fff"]},"movie":{}}""");
        var source = Source(new(), new());

        Assert.Null(QdrantPayloads.Reconcile(existing, source, false));
        Assert.NotNull(QdrantPayloads.Reconcile(existing, source, true));
    }

    [Fact]
    public void Differences_TreatsExplicitNullAsDifferentFromMissing()
    {
        var existing = FromJson("""{"image":{"lens":null},"movie":{}}""");
        var diffs = QdrantPayloads.Differences(existing, Source(new(), new()), false);
        Assert.Single(diffs);
        Assert.Equal("null", diffs[0].Old);
    }

    [Fact]
    public void Equivalent_ComparesNumbersByValue()
    {
        Assert.True(QdrantPayloads.Equivalent(JsonSerializer.Deserialize<JsonElement>("2.50"), 2.5m));
        Assert.False(QdrantPayloads.Equivalent(new[] { "a", "b" }, new[] { "b", "a" }));
    }

    [Fact]
    public void Formatting_MatchesImporter()
    {
        Assert.Equal("2014-10-21T00:00:00.0000000+00:00", QdrantPayloads.IsoTimestamp("2014-10-21"));
        Assert.Null(QdrantPayloads.IsoTimestamp(" "));

        var payload = new Dictionary<string, object?>();
        QdrantPayloads.PutHierarchy(payload, "location", "USA: California : usa");
        Assert.Equal("USA: California : usa", payload["location_raw"]);
        Assert.Equal(new[] { "USA", "California" }, (string[])payload["location"]!);

        QdrantPayloads.PutCommaArray(payload, "actors", " A, B ,a,");
        Assert.Equal(new[] { "A", "B" }, (string[])payload["actors"]!);
    }

    [Theory]
    [InlineData("2026-11-09T08:59:00", null)]
    [InlineData("2026-11-09T09:00:00", "2026-11-09")]
    [InlineData("2026-11-09T14:59:00", "2026-11-09")]
    [InlineData("2026-11-09T15:00:00", null)]
    public void NightlyDue_OnlyInsideWindow(string now, string? expected)
    {
        var nightly = new QdrantSyncOptions.NightlyOptions { AtUtc = "09:00", WindowHours = 6 };
        var due = QdrantSyncWorker.NightlyDue(nightly, DateTime.SpecifyKind(DateTime.Parse(now), DateTimeKind.Utc));
        Assert.Equal(expected, due?.ToString("yyyy-MM-dd"));
    }

    [Fact]
    public void Report_WritesBothCsvs()
    {
        var job = new QdrantSyncJob
        {
            Id = 7,
            FieldCounts = new() { ["image.lens"] = 3, ["movie.title"] = 9 },
            Examples = [new ChangeExample(42, "movie.title", "\"Old, \"\"x\"\"\"", "\"New\"")],
        };

        Assert.Equal("field,records_changed\n\"movie.title\",9\n\"image.lens\",3\n", QdrantSyncReport.ByFieldCsv(job));
        var examples = QdrantSyncReport.ExamplesCsv(job).Split('\n');
        Assert.Equal("image_id,field,qdrant_old,postgres_new", examples[0]);
        Assert.Equal("42,\"movie.title\",\"\"\"Old, \"\"\"\"x\"\"\"\"\"\"\",\"\"\"New\"\"\"", examples[1]);
        Assert.Equal("qdrant-sync-7-changes-examples.csv", QdrantSyncReport.ExamplesFileName(job));
    }

    private sealed class FakeHandler(Func<HttpRequestMessage, string, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public List<string> Bodies { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var body = request.Content == null ? "" : await request.Content.ReadAsStringAsync(ct);
            Bodies.Add(body);
            return respond(request, body);
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode code, string json) =>
        new(code) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    [Fact]
    public async Task RestClient_RetriesServerErrors_AndNeverRequestsVectors()
    {
        var calls = 0;
        var handler = new FakeHandler((_, _) => ++calls == 1
            ? Json(HttpStatusCode.ServiceUnavailable, "{}")
            : Json(HttpStatusCode.OK, """{"status":"ok","result":[{"id":3,"payload":{"image":{"movieid":1}}}]}"""));
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);

        var found = await client.GetPayloadsAsync("images", [3, 4], CancellationToken.None);

        Assert.Equal(2, calls);
        Assert.Equal([3L], found.Keys);
        using var body = JsonDocument.Parse(handler.Bodies[0]);
        Assert.False(body.RootElement.GetProperty("with_vector").GetBoolean());
    }

    [Fact]
    public async Task RestClient_DoesNotRetryClientErrors()
    {
        var calls = 0;
        var handler = new FakeHandler((_, _) => { calls++; return Json(HttpStatusCode.BadRequest, """{"status":"bad"}"""); });
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);

        await Assert.ThrowsAsync<IOException>(() => client.SetPayloadsAsync("images", [(1L, new Dictionary<string, object?>())], CancellationToken.None));
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task RestClient_SetPayload_SendsOnlyPayloadOperations()
    {
        var handler = new FakeHandler((req, _) =>
        {
            Assert.Equal("/collections/images/points/batch", req.RequestUri!.AbsolutePath);
            return Json(HttpStatusCode.OK, """{"status":"ok","result":[]}""");
        });
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);

        await client.SetPayloadsAsync("images", [(9L, new Dictionary<string, object?> { ["image"] = new Dictionary<string, object?> { ["lens"] = "x" } })], CancellationToken.None);

        Assert.DoesNotContain("vector", handler.Bodies[0]);
        using var body = JsonDocument.Parse(handler.Bodies[0]);
        var op = body.RootElement.GetProperty("operations")[0].GetProperty("set_payload");
        Assert.Equal(9, op.GetProperty("points")[0].GetInt64());
        Assert.Equal("x", op.GetProperty("payload").GetProperty("image").GetProperty("lens").GetString());
    }
}
