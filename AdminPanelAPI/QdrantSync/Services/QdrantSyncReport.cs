using System.Globalization;
using System.Text;
using AdminPanelAPI.QdrantSync.Models;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>The two CSVs the console sync wrote, built from a job row.</summary>
public static class QdrantSyncReport
{
    public static string ExamplesFileName(QdrantSyncJob job) => $"qdrant-sync-{job.Id}-changes-examples.csv";
    public static string ByFieldFileName(QdrantSyncJob job) => $"qdrant-sync-{job.Id}-changes-by-field.csv";

    /// <summary>image_id,field,qdrant_old,postgres_new for the first changed records.</summary>
    public static string ExamplesCsv(QdrantSyncJob job)
    {
        var sb = new StringBuilder("image_id,field,qdrant_old,postgres_new\n");
        foreach (var e in job.Examples ?? new())
            sb.Append(e.ImageId.ToString(CultureInfo.InvariantCulture)).Append(',')
              .Append(Csv(e.Field)).Append(',').Append(Csv(e.Qdrant)).Append(',').Append(Csv(e.Postgres)).Append('\n');
        return sb.ToString();
    }

    /// <summary>field,records_changed, most changed first.</summary>
    public static string ByFieldCsv(QdrantSyncJob job)
    {
        var sb = new StringBuilder("field,records_changed\n");
        foreach (var (field, count) in job.FieldCounts.OrderByDescending(x => x.Value).ThenBy(x => x.Key, StringComparer.Ordinal))
            sb.Append(Csv(field)).Append(',').Append(count.ToString(CultureInfo.InvariantCulture)).Append('\n');
        return sb.ToString();
    }

    public static bool HasCsvs(QdrantSyncJob job) => job.Kind != "insert";

    public static string Subject(QdrantSyncJob job) => job.Kind == "insert"
        ? $"Qdrant insert #{job.Id} {job.Status}: {job.Written:N0} inserted, {job.Missing:N0} missing{(job.Apply ? "" : " (dry run)")}"
        : $"Qdrant sync #{job.Id} {job.Status}: {job.Changed:N0} changed{(job.Apply ? "" : " (dry run)")}";

    public static string Body(QdrantSyncJob job)
    {
        if (job.Kind == "insert") return InsertBody(job);
        var elapsed = job.StartedAt != null && job.FinishedAt != null ? job.FinishedAt.Value - job.StartedAt.Value : (TimeSpan?)null;
        var sb = new StringBuilder();
        sb.AppendLine($"Qdrant metadata sync #{job.Id} ({job.TriggeredBy}{(job.NightlyDate is { } d ? $" {d:yyyy-MM-dd}" : "")})");
        sb.AppendLine($"Status: {job.Status}{(job.Apply ? "" : " — dry run, nothing written to Qdrant")}");
        if (job.Error != null) sb.AppendLine($"Error: {job.Error}");
        sb.AppendLine();
        sb.AppendLine($"Scanned (live in Postgres): {job.Scanned:N0}");
        sb.AppendLine($"In Qdrant:                  {job.Found:N0}");
        sb.AppendLine($"Not in Qdrant:              {job.Missing:N0}");
        sb.AppendLine($"Changed:                    {job.Changed:N0}");
        sb.AppendLine($"Unchanged:                  {job.Unchanged:N0}");
        sb.AppendLine($"Written:                    {job.Written:N0}");
        sb.AppendLine($"Last image id:              {job.LastId}");
        if (elapsed != null) sb.AppendLine($"Elapsed:                    {elapsed.Value:hh\\:mm\\:ss}");
        if (job.FieldCounts.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("Records changed by field (top 15):");
            foreach (var (field, count) in job.FieldCounts.OrderByDescending(x => x.Value).Take(15))
                sb.AppendLine($"  {field,-38} {count:N0}");
        }
        sb.AppendLine();
        sb.AppendLine($"Attached: {ExamplesFileName(job)} (first {job.Examples?.Select(e => e.ImageId).Distinct().Count() ?? 0:N0} changed records) and {ByFieldFileName(job)}.");
        return sb.ToString();
    }

    private static string InsertBody(QdrantSyncJob job)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Qdrant insert-missing #{job.Id} ({job.TriggeredBy}{(job.NightlyDate is { } d ? $" {d:yyyy-MM-dd}" : "")})");
        sb.AppendLine($"Status: {job.Status}{(job.Apply ? "" : " — dry run, nothing inserted")}");
        if (job.Error != null) sb.AppendLine($"Error: {job.Error}");
        sb.AppendLine();
        sb.AppendLine($"Embedded images in Postgres: {job.Scanned:N0}");
        sb.AppendLine($"Already in Qdrant:           {job.Found:N0}");
        sb.AppendLine($"Missing from Qdrant:         {job.Missing:N0}");
        sb.AppendLine($"Inserted:                    {job.Written:N0}");
        if (job.StartedAt != null && job.FinishedAt != null)
            sb.AppendLine($"Elapsed:                     {job.FinishedAt.Value - job.StartedAt.Value:hh\\:mm\\:ss}");
        return sb.ToString();
    }

    private static string Csv(string text) =>
        "\"" + text.Replace("\"", "\"\"").Replace("\r", " ").Replace("\n", " ") + "\"";
}
