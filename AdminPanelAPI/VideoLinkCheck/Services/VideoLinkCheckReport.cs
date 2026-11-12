using System.Globalization;
using System.Net.Mail;
using System.Text;
using AdminPanelAPI.VideoLinkCheck.Models;

namespace AdminPanelAPI.VideoLinkCheck.Services;

/// <summary>The link-check CSV (same columns as the console checker, plus previous_status) and email text.</summary>
public static class VideoLinkCheckReport
{
    public static readonly string[] Header =
        { "id", "title", "media_type", "url", "platform", "video_id", "status", "checked_at_iso", "note", "previous_status" };

    public static string FileName(VideoLinkCheckJob job, bool issuesOnly) =>
        $"video-links-{job.Id}-{(job.FinishedAt ?? job.CreatedAt):yyyy-MM-dd}-{(issuesOnly ? "issues" : "all")}.csv";

    public static string Csv(IEnumerable<LinkCheckResult> results)
    {
        var sb = new StringBuilder();
        AppendRow(sb, Header);
        foreach (var r in results)
            AppendRow(sb, new[]
            {
                r.MovieId.ToString(CultureInfo.InvariantCulture), r.Title, r.MediaType, r.Url, r.Platform, r.VideoId,
                r.Status, DateTime.SpecifyKind(r.CheckedAt, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
                r.Note ?? "", r.PreviousStatus ?? "",
            });
        return sb.ToString();
    }

    private static void AppendRow(StringBuilder sb, string[] cells)
    {
        for (var i = 0; i < cells.Length; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append(Escape(cells[i]));
        }
        sb.Append("\r\n");
    }

    public static string Escape(string value) =>
        value.IndexOfAny(new[] { ',', '"', '\n', '\r' }) >= 0 ? "\"" + value.Replace("\"", "\"\"") + "\"" : value;

    /// <summary>Unavailable now, active in the previous check.</summary>
    public static bool IsNewlyUnavailable(LinkCheckResult r) => r.Status == "unavailable" && r.PreviousStatus == "active";

    public static string Subject(VideoLinkCheckJob job) =>
        $"Video link check #{job.Id} {job.Status}: {job.Unavailable:N0} unavailable" +
        (job.PreviousJobId != null ? $" ({job.NewlyUnavailable:N0} new)" : "") +
        $" of {job.Scanned:N0}";

    public static string Body(VideoLinkCheckJob job, IReadOnlyList<LinkCheckResult> issues, string csvUrl, int maxListed = 100)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Music video / commercial link check #{job.Id} ({job.TriggeredBy})");
        sb.AppendLine($"Status: {job.Status}");
        if (job.Error != null) sb.AppendLine($"Error: {job.Error}");
        if (job.StartedAt is { } s && job.FinishedAt is { } f) sb.AppendLine($"Took: {(f - s):hh\\:mm\\:ss}");
        sb.AppendLine();
        sb.AppendLine($"Checked:          {job.Scanned:N0}");
        sb.AppendLine($"Active:           {job.Active:N0}");
        sb.AppendLine($"Unavailable:      {job.Unavailable:N0}");
        if (job.PreviousJobId is { } prev)
            sb.AppendLine($"  new since #{prev}: {job.NewlyUnavailable:N0}");
        sb.AppendLine($"Unknown platform: {job.UnknownPlatform:N0}");
        sb.AppendLine($"Errors:           {job.Errors:N0}");

        var newly = issues.Where(IsNewlyUnavailable).ToList();
        if (newly.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine($"Newly unavailable{(newly.Count > maxListed ? $" (first {maxListed} of {newly.Count:N0})" : "")}:");
            foreach (var r in newly.Take(maxListed))
                sb.AppendLine($"  {r.MovieId}  {r.Title} [{r.MediaType}]  {r.Url}  ({r.Note})");
        }

        sb.AppendLine();
        sb.AppendLine("Attached: every link that isn't active, and the full list.");
        sb.AppendLine($"Full CSV: {csvUrl}");
        return sb.ToString();
    }

    /// <summary>Splits on , or ; and validates each address. Null when blank.</summary>
    public static string? NormalizeRecipients(string? value, out string? invalid)
    {
        invalid = null;
        if (string.IsNullOrWhiteSpace(value)) return null;
        var list = new List<string>();
        foreach (var part in value.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (!MailAddress.TryCreate(part, out var address) || address.Address != part)
            {
                invalid = part;
                return null;
            }
            if (!list.Contains(address.Address, StringComparer.OrdinalIgnoreCase)) list.Add(address.Address);
        }
        return list.Count == 0 ? null : string.Join(",", list);
    }
}
