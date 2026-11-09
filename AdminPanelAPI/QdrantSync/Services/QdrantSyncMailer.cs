using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;
using AdminPanelAPI.QdrantSync.Models;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>Emails a finished job's summary with both CSVs attached, over SMTP (STARTTLS).</summary>
public sealed class QdrantSyncMailer(
    IOptionsMonitor<QdrantSyncOptions> options, QdrantSyncStore store, ILogger<QdrantSyncMailer> logger)
{
    public bool Configured => options.CurrentValue.Email.Configured;

    /// <summary>Sends (or skips when unconfigured) and saves the outcome on the job's email_status.</summary>
    public async Task<string> SendAndRecordAsync(QdrantSyncJob job, CancellationToken ct)
    {
        string status;
        if (!Configured)
        {
            status = "skipped: QdrantSync:Email not configured";
        }
        else
        {
            try
            {
                await SendAsync(job, ct);
                status = $"sent to {options.CurrentValue.Email.To} at {DateTime.UtcNow:yyyy-MM-dd HH:mm}Z";
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Qdrant sync #{JobId} report email failed.", job.Id);
                status = $"failed: {SmtpDiagnostics.Describe(ex)}";
            }
        }
        await store.SetEmailStatusAsync(job.Id, status, CancellationToken.None);
        job.EmailStatus = status;
        return status;
    }

    public async Task SendAsync(QdrantSyncJob job, CancellationToken ct)
    {
        var cfg = options.CurrentValue.Email;
        if (!cfg.Configured) throw new InvalidOperationException("QdrantSync:Email is not configured (Password and To are required).");
        var username = string.IsNullOrWhiteSpace(cfg.Username) ? cfg.From : cfg.Username;
        var from = string.IsNullOrWhiteSpace(cfg.From) ? username : cfg.From;

        using var message = new MailMessage
        {
            From = new MailAddress(from, "ShotDeck Qdrant Sync"),
            Subject = QdrantSyncReport.Subject(job),
            Body = QdrantSyncReport.Body(job),
            BodyEncoding = Encoding.UTF8,
            SubjectEncoding = Encoding.UTF8,
        };
        foreach (var to in cfg.To.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            message.To.Add(to);
        if (QdrantSyncReport.HasCsvs(job))
        {
            message.Attachments.Add(Csv(QdrantSyncReport.ExamplesCsv(job), QdrantSyncReport.ExamplesFileName(job)));
            message.Attachments.Add(Csv(QdrantSyncReport.ByFieldCsv(job), QdrantSyncReport.ByFieldFileName(job)));
        }

        using var smtp = new SmtpClient(cfg.SmtpHost, cfg.SmtpPort)
        {
            EnableSsl = true,
            DeliveryMethod = SmtpDeliveryMethod.Network,
            Credentials = new NetworkCredential(username, cfg.Password.Replace(" ", "")),
            Timeout = 100_000,
        };
        await smtp.SendMailAsync(message, ct);
    }

    private static Attachment Csv(string csv, string name) =>
        new(new MemoryStream(Encoding.UTF8.GetBytes(csv)), name, MediaTypeNames.Text.Csv);
}
