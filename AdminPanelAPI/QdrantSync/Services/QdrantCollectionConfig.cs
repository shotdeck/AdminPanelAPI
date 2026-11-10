using System.Text.Json;
using System.Text.Json.Nodes;
using AdminPanelAPI.QdrantSync.Models;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>Reads a Qdrant collection's info and builds the config for the rebuild target.</summary>
public static class QdrantCollectionConfig
{
    private static readonly string[] OptimizerKeys =
    {
        "deleted_threshold", "vacuum_min_vector_number", "default_segment_number", "max_segment_size",
        "memmap_threshold", "indexing_threshold", "flush_interval_sec", "max_optimization_threads",
    };
    private static readonly string[] ParamKeys =
    {
        "shard_number", "replication_factor", "write_consistency_factor", "on_disk_payload", "sparse_vectors",
    };
    public const int DefaultIndexingThreshold = 10000;

    /// <summary>
    /// Create-collection body: the source's vectors, shards, replication, HNSW, quantization and strict
    /// mode, plus the cinemanet vector. Indexing is off (threshold 0) for the bulk copy; the rebuild
    /// turns it back on when the copy finishes.
    /// </summary>
    public static JsonObject RebuildTarget(JsonElement sourceInfo, QdrantSyncOptions opts)
    {
        var config = sourceInfo.GetProperty("config");
        var p = config.GetProperty("params");
        var vectors = Node(p.GetProperty("vectors"))!.AsObject();
        vectors[opts.CinemanetVectorName] = new JsonObject
        {
            ["size"] = opts.CinemanetVectorSize,
            ["distance"] = opts.CinemanetDistance,
            ["on_disk"] = opts.CinemanetOnDisk,
        };
        var body = new JsonObject { ["vectors"] = vectors };
        foreach (var key in ParamKeys)
            if (p.TryGetProperty(key, out var v) && Node(v) is { } n) body[key] = n;
        foreach (var key in new[] { "hnsw_config", "quantization_config", "strict_mode_config" })
            if (config.TryGetProperty(key, out var v) && Node(v) is { } n) body[key] = n;

        var optimizers = new JsonObject();
        if (config.TryGetProperty("optimizer_config", out var o) && o.ValueKind == JsonValueKind.Object)
            foreach (var key in OptimizerKeys)
                if (o.TryGetProperty(key, out var v) && Node(v) is { } n) optimizers[key] = n;
        optimizers["indexing_threshold"] = 0;
        body["optimizers_config"] = optimizers;
        return body;
    }

    /// <summary>The source's indexing threshold, to restore after the bulk copy.</summary>
    public static int IndexingThreshold(JsonElement info) =>
        info.GetProperty("config").TryGetProperty("optimizer_config", out var o)
        && o.TryGetProperty("indexing_threshold", out var t) && t.ValueKind == JsonValueKind.Number && t.GetInt32() > 0
            ? t.GetInt32()
            : DefaultIndexingThreshold;

    public static HashSet<string> VectorNames(JsonElement info)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        if (info.GetProperty("config").GetProperty("params").GetProperty("vectors") is { ValueKind: JsonValueKind.Object } vectors)
            foreach (var v in vectors.EnumerateObject())
                if (v.Value.ValueKind == JsonValueKind.Object && v.Value.TryGetProperty("size", out _)) names.Add(v.Name);
        return names;
    }

    /// <summary>Payload index field → the <c>field_schema</c> to create it with.</summary>
    public static Dictionary<string, JsonElement> PayloadIndexes(JsonElement info)
    {
        var result = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        if (!info.TryGetProperty("payload_schema", out var schema) || schema.ValueKind != JsonValueKind.Object) return result;
        foreach (var field in schema.EnumerateObject())
        {
            var f = field.Value;
            result[field.Name] = f.TryGetProperty("params", out var prm) && prm.ValueKind == JsonValueKind.Object
                ? prm.Clone()
                : f.GetProperty("data_type").Clone();
        }
        return result;
    }

    /// <summary>A JSON copy with null-valued properties dropped (Qdrant rejects some explicit nulls).</summary>
    private static JsonNode? Node(JsonElement e)
    {
        switch (e.ValueKind)
        {
            case JsonValueKind.Null or JsonValueKind.Undefined:
                return null;
            case JsonValueKind.Object:
                var obj = new JsonObject();
                foreach (var prop in e.EnumerateObject())
                    if (Node(prop.Value) is { } n) obj[prop.Name] = n;
                return obj;
            case JsonValueKind.Array:
                var arr = new JsonArray();
                foreach (var item in e.EnumerateArray()) arr.Add(Node(item));
                return arr;
            default:
                return JsonNode.Parse(e.GetRawText());
        }
    }
}
