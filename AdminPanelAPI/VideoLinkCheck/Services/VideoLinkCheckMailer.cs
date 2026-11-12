using System.IO.Compression;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;
using AdminPanelAPI.QdrantSync.Models;
using AdminPanelAPI.QdrantSync.Services;
using AdminPanelAPI.VideoLinkCheck.Models;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.VideoLinkCheck.Services;

/// <summary>Emails a finished check's summary with the issues CSV and the full CSV attached (Gmail SMTP, STARTTLS).</summary>
public sealed class VideoLinkCheckMailer(
    IOptionsMonitor<VideoLinkCheckOptions> options,
    IOptionsMonitor<QdrantSyncOptions> qdrantOptions,
    VideoLinkCheckStore store,
    ILogger<VideoLinkCheckMailer> logger)
{
    /// <summary>Full CSVs larger than this are zipped before attaching.</summary>
    private const int ZipAboveBytes = 5 * 1024 * 1024;

    public sealed record Smtp(string Host, int Port, string Username, string Password, string From, string To)
    {
        public bool Configured => !string.IsNullOrWhiteSpace(Host) && !string.IsNullOrWhiteSpace(Password);
    }

    /// <summary>VideoLinkCheck:Email, with blank SMTP fields taken from QdrantSync:Email (To is never borrowed).</summary>
    public static Smtp Resolve(VideoLinkCheckOptions.EmailOptions own, QdrantSyncOptions.EmailOptions shared)
    {
        static string Pick(string a, string b) => string.IsNullOrWhiteSpace(a) ? b : a;
        var username = Pick(own.Username, Pick(shared.Username, Pick(own.From, shared.From)));
        return new Smtp(
            Pick(own.SmtpHost, shared.SmtpHost),
            own.SmtpPort > 0 ? own.SmtpPort : shared.SmtpPort,
            username,
            Pick(own.Password, shared.Password),
            Pick(own.From, Pick(shared.From, username)),
            own.To);
    }

    public Smtp Settings => Resolve(options.CurrentValue.Email, qdrantOptions.CurrentValue.Email);

    /// <summary>Sends to <paramref name="to"/>, else the job's recipients, else VideoLinkCheck:Email:To, and records the outcome.</summary>
    public async Task<string> SendAndRecordAsync(VideoLinkCheckJob job, string? to, CancellationToken ct)
    {
        var smtp = Settings;
        var recipients = to ?? job.EmailTo ?? smtp.To;
        string status;
        if (!smtp.Configured)
            status = "skipped: no SMTP password (VideoLinkCheck:Email or QdrantSync:Email)";
        else if (string.IsNullOrWhiteSpace(recipients))
            status = "skipped: no recipients (pass ?to= or set VideoLinkCheck:Email:To)";
        else
        {
            try
            {
                await SendAsync(job, smtp, recipients, ct);
                status = $"sent to {recipients} at {DateTime.UtcNow:yyyy-MM-dd HH:mm}Z";
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Video link check #{JobId} report email failed.", job.Id);
                status = $"failed: {SmtpDiagnostics.Describe(ex)}";
            }
        }
        await store.SetEmailStatusAsync(job.Id, status, CancellationToken.None);
        job.EmailStatus = status;
        return status;
    }

    private async Task SendAsync(VideoLinkCheckJob job, Smtp smtp, string recipients, CancellationToken ct)
    {
        var all = await store.GetResultsAsync(job, issuesOnly: false, ct);
        var issues = all.Where(r => r.Status != "active").ToList();
        var csvUrl = $"{options.CurrentValue.PublicBaseUrl.TrimEnd('/')}/api/video-link-check/jobs/{job.Id}/results.csv";

        using var message = new MailMessage
        {
            From = new MailAddress(smtp.From, "ShotDeck Video Link Check"),
            Subject = VideoLinkCheckReport.Subject(job),
            Body = VideoLinkCheckReport.Body(job, issues, csvUrl),
            BodyEncoding = Encoding.UTF8,
            SubjectEncoding = Encoding.UTF8,
        };
        foreach (var address in recipients.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            message.To.Add(address);

        message.Attachments.Add(Csv(VideoLinkCheckReport.Csv(issues), VideoLinkCheckReport.FileName(job, issuesOnly: true)));
        var full = Encoding.UTF8.GetBytes(VideoLinkCheckReport.Csv(all));
        var fullName = VideoLinkCheckReport.FileName(job, issuesOnly: false);
        message.Attachments.Add(full.Length > ZipAboveBytes
            ? new Attachment(new MemoryStream(Zip(full, fullName)), Path.ChangeExtension(fullName, ".zip"), MediaTypeNames.Application.Zip)
            : new Attachment(new MemoryStream(WithBom(full)), fullName, MediaTypeNames.Text.Csv));

        using var client = new SmtpClient(smtp.Host, smtp.Port)
        {
            EnableSsl = true,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Credentials = new NetworkCredential(smtp.Username, QdrantSyncMailer.NormalizePassword(smtp.Password)),
            Timeout = 100_000,
        };
        await client.SendMailAsync(message, ct);
    }

    /// <summary>UTF-8 BOM so Excel shows accented titles correctly.</summary>
    private static byte[] WithBom(byte[] bytes) => Encoding.UTF8.GetPreamble().Concat(bytes).ToArray();

    private static Attachment Csv(string csv, string name) =>
        new(new MemoryStream(WithBom(Encoding.UTF8.GetBytes(csv))), name, MediaTypeNames.Text.Csv);

    private static byte[] Zip(byte[] csv, string entryName)
    {
        using var ms = new MemoryStream();
        using (var zip = new ZipArchive(ms, ZipArchiveMode.Create, leaveOpen: true))
        {
            using var entry = zip.CreateEntry(entryName, CompressionLevel.Optimal).Open();
            entry.Write(WithBom(csv));
        }
        return ms.ToArray();
    }
}
