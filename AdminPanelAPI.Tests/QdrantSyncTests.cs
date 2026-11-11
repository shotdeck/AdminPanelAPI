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
    public void Reconcile_AddsNewImageAndMovieFields_AndDropsClipWhenBoundaryGone()
    {
        var existing = FromJson("""{"image":{"movieid":5,"clip_start":1.5,"clip_end":3.0},"movie":{"title":"The Thing"}}""");
        var source = Source(
            new() { ["movieid"] = 5L, ["filename"] = "ABC123.png", ["created"] = "2014-10-22T01:11:00.0000000+00:00" },
            new()
            {
                ["title"] = "The Thing", ["alphatitle"] = "Thing", ["tv_episode"] = 3L, ["tv_episode_title"] = "Pilot",
                ["poster"] = "p.jpg", ["key_image"] = "k.jpg", ["video_link"] = "https://v", ["video_file"] = "m.mp4",
                ["content_status"] = "live"
            });

        Assert.NotNull(QdrantPayloads.Reconcile(existing, source, false));
        var fields = QdrantPayloads.Differences(existing, source, false).Select(d => d.Field).ToHashSet();
        Assert.Equal(new HashSet<string>
        {
            "image.filename", "image.created", "image.clip_start", "image.clip_end", "movie.alphatitle", "movie.tv_episode",
            "movie.tv_episode_title", "movie.poster", "movie.key_image", "movie.video_link", "movie.video_file", "movie.content_status"
        }, fields);
        Assert.Equal("datetime", QdrantPayloads.RequiredIndexes["image.created"]);
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

    [Fact]
    public void ParseVector_RequiresExactly998FiniteValues()
    {
        var literal = "[" + string.Join(",", Enumerable.Range(0, 998).Select(i => (i / 1000.0).ToString(System.Globalization.CultureInfo.InvariantCulture))) + "]";
        var v = QdrantPayloads.ParseVector(literal);
        Assert.Equal(998, v.Length);
        Assert.Equal(0.997f, v[^1]);
        Assert.Throws<FormatException>(() => QdrantPayloads.ParseVector("[1,2,3]"));
        Assert.Throws<FormatException>(() => QdrantPayloads.ParseVector("1,2,3"));
    }

    [Fact]
    public async Task RestClient_ScrollsUntilNoNextOffset()
    {
        var pages = new Queue<string>(new[]
        {
            """{"status":"ok","result":{"points":[{"id":1},{"id":2}],"next_page_offset":3}}""",
            """{"status":"ok","result":{"points":[{"id":3}],"next_page_offset":null}}""",
        });
        var handler = new FakeHandler((_, _) => Json(HttpStatusCode.OK, pages.Dequeue()));
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);

        var ids = await client.ScrollAllIdsAsync("images", 2, CancellationToken.None);

        Assert.Equal(new long[] { 1, 2, 3 }, ids.OrderBy(x => x));
        Assert.Equal(3, JsonDocument.Parse(handler.Bodies[1]).RootElement.GetProperty("offset").GetInt64());
    }

    [Fact]
    public async Task RestClient_UpsertSendsNamedVector()
    {
        var handler = new FakeHandler((req, _) =>
        {
            Assert.Equal(HttpMethod.Put, req.Method);
            return Json(HttpStatusCode.OK, """{"status":"ok","result":{}}""");
        });
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);
        var point = new ImagePoint(5, new Dictionary<string, object?> { ["image"] = new Dictionary<string, object?>() }, [0.5f, 1f]);

        await client.UpsertPointsAsync("images", "qwen3", [point], CancellationToken.None);

        using var body = JsonDocument.Parse(handler.Bodies[0]);
        var p = body.RootElement.GetProperty("points")[0];
        Assert.Equal(5, p.GetProperty("id").GetInt64());
        Assert.Equal(2, p.GetProperty("vector").GetProperty("qwen3").GetArrayLength());
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

    [Theory]
    [InlineData("sync", "done", true, 0, 0, 0, true)]
    [InlineData("sync", "done", false, 5, 0, 0, false)]
    [InlineData("sync", "done", true, 0, 3, 0, false)]
    [InlineData("sync", "done", true, 0, 0, 12, true)]
    [InlineData("sync", "failed", true, 0, 0, 0, false)]
    [InlineData("insert", "done", true, 0, 0, 0, true)]
    [InlineData("insert", "done", false, 0, 0, 8, false)]
    [InlineData("insert", "done", true, 0, 8, 8, false)]
    [InlineData("rebuild", "done", false, 0, 0, 0, false)]
    public void Report_NothingToReport_OnlyForDoneJobsWithNoChanges(
        string kind, string status, bool apply, long changed, long written, long missing, bool expected)
    {
        var job = new QdrantSyncJob { Kind = kind, Status = status, Apply = apply, Changed = changed, Written = written, Missing = missing };
        Assert.Equal(expected, QdrantSyncReport.NothingToReport(job));
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

    [Fact]
    public void ParseVector_CinemanetSizeAndFiniteValues()
    {
        var text = "[" + string.Join(",", Enumerable.Repeat("0.25", 1280)) + "]";
        Assert.Equal(1280, QdrantPayloads.ParseVector(text, QdrantPayloads.CinemanetVectorSize).Length);
        var qwen = "[" + string.Join(",", Enumerable.Repeat("1", 998)) + "]";
        Assert.Throws<FormatException>(() => QdrantPayloads.ParseVector(qwen, QdrantPayloads.CinemanetVectorSize));
        var nan = "[NaN" + string.Concat(Enumerable.Repeat(",1", 1279)) + "]";
        Assert.Throws<FormatException>(() => QdrantPayloads.ParseVector(nan, QdrantPayloads.CinemanetVectorSize));
    }

    [Fact]
    public async Task RestClient_UpsertAddsCinemanetOnlyWhenPresent()
    {
        var handler = new FakeHandler((_, _) => Json(HttpStatusCode.OK, """{"status":"ok","result":{}}"""));
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);
        var with = new ImagePoint(1, new Dictionary<string, object?> { ["image"] = new Dictionary<string, object?>() }, [1f]) { Cinemanet = [2f, 3f] };
        var without = new ImagePoint(2, new Dictionary<string, object?> { ["image"] = new Dictionary<string, object?>() }, [1f]);
        await client.UpsertPointsAsync("images", "qwen3", [with, without], CancellationToken.None, "cinemanet");
        using var body = JsonDocument.Parse(handler.Bodies[0]);
        var points = body.RootElement.GetProperty("points");
        Assert.Equal(2, points[0].GetProperty("vector").GetProperty("cinemanet").GetArrayLength());
        Assert.True(points[0].GetProperty("vector").TryGetProperty("qwen3", out _));
        Assert.False(points[1].GetProperty("vector").TryGetProperty("cinemanet", out _));
    }

    [Fact]
    public async Task RestClient_UpdateVectorsSendsOnlyThatVector()
    {
        string? url = null;
        var handler = new FakeHandler((req, _) =>
        {
            url = req.RequestUri!.PathAndQuery;
            Assert.Equal(HttpMethod.Put, req.Method);
            return Json(HttpStatusCode.OK, """{"status":"ok","result":{}}""");
        });
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);
        await client.UpdateVectorsAsync("images", "cinemanet", [(7L, new[] { 1f, 2f })], CancellationToken.None);
        Assert.StartsWith("/collections/images/points/vectors?", url);
        using var body = JsonDocument.Parse(handler.Bodies[0]);
        var p = body.RootElement.GetProperty("points")[0];
        Assert.Equal(7, p.GetProperty("id").GetInt64());
        Assert.False(p.TryGetProperty("payload", out _));
        var vector = p.GetProperty("vector");
        Assert.Single(vector.EnumerateObject());
        Assert.Equal(2, vector.GetProperty("cinemanet").GetArrayLength());
    }

    [Fact]
    public async Task RestClient_IdsWithoutVectorFiltersByHasVector()
    {
        var handler = new FakeHandler((_, _) =>
            Json(HttpStatusCode.OK, """{"status":"ok","result":{"points":[{"id":3}],"next_page_offset":null}}"""));
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);
        var lacking = await client.IdsWithoutVectorAsync("images", "cinemanet", [1L, 2L, 3L], CancellationToken.None);
        Assert.Equal(new HashSet<long> { 3 }, lacking);
        using var body = JsonDocument.Parse(handler.Bodies[0]);
        var filter = body.RootElement.GetProperty("filter");
        Assert.Equal(3, filter.GetProperty("must")[0].GetProperty("has_id").GetArrayLength());
        Assert.Equal("cinemanet", filter.GetProperty("must_not")[0].GetProperty("has_vector").GetString());
        Assert.Equal(3, body.RootElement.GetProperty("limit").GetInt32());
    }

    [Fact]
    public async Task RestClient_ScrollPageReadsNamedVectorsAndPayload()
    {
        var handler = new FakeHandler((_, _) => Json(HttpStatusCode.OK,
            """{"status":"ok","result":{"points":[{"id":10,"vector":{"qwen3":[0.5,1]},"payload":{"image":{"id":10}}}],"next_page_offset":11}}"""));
        var client = new QdrantRestClient(new HttpClient(handler) { BaseAddress = new Uri("http://q/") }, NullLogger.Instance);
        var (points, next) = await client.ScrollPageAsync("images", 10, 1, true, CancellationToken.None);
        Assert.Equal(11, next);
        Assert.Equal(new[] { 0.5f, 1f }, points[0].Vectors["qwen3"]);
        Assert.Equal(10, points[0].Payload!.Value.GetProperty("image").GetProperty("id").GetInt32());
    }

    [Fact]
    public void RebuildTarget_CopiesSourceConfigAndAddsCinemanet()
    {
        using var info = JsonDocument.Parse("""
            {"config":{"params":{"vectors":{"qwen3":{"size":998,"distance":"Cosine","on_disk":false,"multivector_config":null}},
              "shard_number":2,"replication_factor":2,"write_consistency_factor":1,"on_disk_payload":true,"sharding_method":null},
             "hnsw_config":{"m":24,"ef_construct":256,"payload_m":24},
             "optimizer_config":{"deleted_threshold":0.2,"indexing_threshold":20000,"flush_interval_sec":5,"max_optimization_threads":null},
             "quantization_config":null,"strict_mode_config":{"enabled":true}},
             "payload_schema":{"image.movie_id":{"data_type":"integer","points":5},
               "image.title":{"data_type":"text","params":{"type":"text","tokenizer":"word"},"points":5}}}
            """);
        var opts = new QdrantSyncOptions();
        var body = QdrantCollectionConfig.RebuildTarget(info.RootElement, opts);
        Assert.Equal(998, (int)body["vectors"]!["qwen3"]!["size"]!);
        Assert.Null(body["vectors"]!["qwen3"]!.AsObject()["multivector_config"]);
        Assert.Equal(1280, (int)body["vectors"]!["cinemanet"]!["size"]!);
        Assert.Equal("Cosine", (string)body["vectors"]!["cinemanet"]!["distance"]!);
        Assert.True((bool)body["vectors"]!["cinemanet"]!["on_disk"]!);
        Assert.Equal(2, (int)body["replication_factor"]!);
        Assert.Equal(24, (int)body["hnsw_config"]!["m"]!);
        Assert.Equal(0, (int)body["optimizers_config"]!["indexing_threshold"]!);
        Assert.False(body.ContainsKey("quantization_config"));
        Assert.False(body.ContainsKey("sharding_method"));
        Assert.Equal(20000, QdrantCollectionConfig.IndexingThreshold(info.RootElement));

        var indexes = QdrantCollectionConfig.PayloadIndexes(info.RootElement);
        Assert.Equal("integer", indexes["image.movie_id"].GetString());
        Assert.Equal("word", indexes["image.title"].GetProperty("tokenizer").GetString());
    }

    [Fact]
    public void PlanSlices_CoversEveryIdOnceAndLastSliceIsOpenEnded()
    {
        var slices = QdrantSyncRunner.PlanSlices(0, 2_000_000, 16);
        Assert.Equal(16, slices.Count);
        Assert.Equal(0, slices[0].After);
        for (var i = 1; i < slices.Count; i++)
            Assert.Equal(slices[i - 1].End, slices[i].After);
        Assert.Null(slices[^1].End);
        Assert.All(slices, s => Assert.False(s.Done));
    }

    [Theory]
    [InlineData(0, 0, 16)]
    [InlineData(0, 10, 16)]
    [InlineData(500, 2_000_000, 1)]
    [InlineData(2_000_000, 1_000_000, 16)]
    public void PlanSlices_FallsBackToOneOpenSlice(int after, int maxId, int count)
    {
        var slice = Assert.Single(QdrantSyncRunner.PlanSlices(after, maxId, count));
        Assert.Equal(after, slice.After);
        Assert.Null(slice.End);
    }

    [Fact]
    public void PlanSlices_StartsAfterResumePoint()
    {
        var slices = QdrantSyncRunner.PlanSlices(1000, 2000, 4);
        Assert.Equal(new[] { 1000, 1250, 1500, 1750 }, slices.Select(s => s.After));
        Assert.Equal(new int?[] { 1250, 1500, 1750, null }, slices.Select(s => s.End));
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
