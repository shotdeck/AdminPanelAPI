using AdminPanelAPI.QdrantSync.Models;
using AdminPanelAPI.QdrantSync.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.QdrantSync.Controllers;

/// <summary>
/// Postgres → Qdrant metadata sync. Jobs run in the background (<see cref="QdrantSyncWorker"/>)
/// and can take hours; queue one, then poll its status.
/// </summary>
[ApiController]
[Route("api/qdrant-sync")]
public sealed class QdrantSyncController(
    QdrantSyncStore store,
    QdrantSyncMailer mailer,
    IOptionsMonitor<QdrantSyncOptions> options) : ControllerBase
{
    /// <summary>
    /// Queues a full scan of live images that rewrites out-of-date Qdrant payloads from Postgres
    /// (payload only; vectors untouched, images missing from Qdrant are counted, not inserted).
    /// Dry run unless <paramref name="apply"/> is true. Only one job runs at a time.
    /// </summary>
    [HttpPost("sync")]
    public Task<IActionResult> QueueSync(
        [FromQuery] bool apply = false,
        [FromQuery] int startAfterId = 0,
        [FromQuery] long maxRows = 0,
        [FromQuery] bool email = false,
        CancellationToken ct = default) => QueueAsync("sync", apply, startAfterId, maxRows, email, ct);

    /// <summary>
    /// Queues an insert of every image that has a 998-dim embedding but no Qdrant point yet
    /// (vector + full payload). Dry run (counts only) unless <paramref name="apply"/> is true.
    /// <paramref name="maxRows"/> caps how many embedded Postgres ids are scanned.
    /// </summary>
    [HttpPost("insert")]
    public Task<IActionResult> QueueInsert(
        [FromQuery] bool apply = false,
        [FromQuery] int startAfterId = 0,
        [FromQuery] long maxRows = 0,
        [FromQuery] bool email = false,
        CancellationToken ct = default) => QueueAsync("insert", apply, startAfterId, maxRows, email, ct);

    private async Task<IActionResult> QueueAsync(string kind, bool apply, int startAfterId, long maxRows, bool email, CancellationToken ct)
    {
        if (!options.CurrentValue.QdrantConfigured)
            return StatusCode(503, new { error = "QdrantSync:Url and QdrantSync:ApiKey are not configured." });
        if (startAfterId < 0 || maxRows < 0)
            return BadRequest(new { error = "startAfterId and maxRows must be >= 0." });

        await store.EnsureTablesAsync(ct);
        var (jobId, created) = await store.CreateJobAsync(kind, apply, startAfterId, maxRows, email, ct);
        if (!created)
            return Conflict(new { error = "A Qdrant sync/insert job is already queued or running.", jobId, statusUrl = $"/api/qdrant-sync/jobs/{jobId}" });
        return Accepted(new { jobId, kind, apply, startAfterId, maxRows, email, statusUrl = $"/api/qdrant-sync/jobs/{jobId}" });
    }

    [HttpGet("jobs")]
    public async Task<ActionResult<List<QdrantSyncJob>>> ListJobs([FromQuery] int limit = 20, CancellationToken ct = default)
    {
        await store.EnsureTablesAsync(ct);
        return await store.ListJobsAsync(limit, ct);
    }

    /// <summary>Job status and counters; <paramref name="includeExamples"/> adds the field-level before/after rows.</summary>
    [HttpGet("jobs/{id:long}")]
    public async Task<ActionResult<QdrantSyncJob>> GetJob(long id, [FromQuery] bool includeExamples = false, CancellationToken ct = default)
    {
        var job = await store.GetJobAsync(id, ct);
        if (job == null) return NotFound();
        if (!includeExamples) job.Examples = null;
        return job;
    }

    [HttpGet("jobs/{id:long}/changes-examples.csv")]
    public async Task<IActionResult> ExamplesCsv(long id, CancellationToken ct)
    {
        var job = await store.GetJobAsync(id, ct);
        if (job == null) return NotFound();
        return File(System.Text.Encoding.UTF8.GetBytes(QdrantSyncReport.ExamplesCsv(job)), "text/csv", QdrantSyncReport.ExamplesFileName(job));
    }

    [HttpGet("jobs/{id:long}/changes-by-field.csv")]
    public async Task<IActionResult> ByFieldCsv(long id, CancellationToken ct)
    {
        var job = await store.GetJobAsync(id, ct);
        if (job == null) return NotFound();
        return File(System.Text.Encoding.UTF8.GetBytes(QdrantSyncReport.ByFieldCsv(job)), "text/csv", QdrantSyncReport.ByFieldFileName(job));
    }

    /// <summary>Cancels a queued job, or stops a running one after its current batch.</summary>
    [HttpPost("jobs/{id:long}/cancel")]
    public async Task<IActionResult> Cancel(long id, CancellationToken ct) =>
        await store.CancelAsync(id, ct) ? Ok(new { id, status = "cancelled" }) : Conflict(new { error = "Job is not queued or running." });

    /// <summary>Emails the job's summary and CSVs now (QdrantSync:Email settings).</summary>
    [HttpPost("jobs/{id:long}/email")]
    public async Task<IActionResult> Email(long id, CancellationToken ct)
    {
        if (!mailer.Configured) return StatusCode(503, new { error = "QdrantSync:Email is not configured." });
        var job = await store.GetJobAsync(id, ct);
        if (job == null) return NotFound();
        var status = await mailer.SendAndRecordAsync(job, ct);
        return status.StartsWith("sent") ? Ok(new { id, emailStatus = status }) : StatusCode(502, new { id, emailStatus = status });
    }

    /// <summary>Which settings are in place (no secrets) and when the nightly run is.</summary>
    /// <summary>
    /// Tests the SMTP path from this server (connect, banner, EHLO, STARTTLS/TLS, AUTH, MAIL FROM/RCPT TO)
    /// on 587 and 465 without sending mail. The password is never echoed, only its fingerprint.
    /// </summary>
    [HttpGet("email/diagnose")]
    public async Task<IActionResult> DiagnoseEmail(CancellationToken ct)
    {
        var e = options.CurrentValue.Email;
        var username = string.IsNullOrWhiteSpace(e.Username) ? e.From : e.Username;
        var login = e.Configured
            ? new SmtpDiagnostics.Login(username, QdrantSyncMailer.NormalizePassword(e.Password), string.IsNullOrWhiteSpace(e.From) ? username : e.From, e.To)
            : null;
        return Ok(await SmtpDiagnostics.RunAsync(e.SmtpHost, new[] { e.SmtpPort, 465 }.Distinct(), ct, login));
    }

    [HttpGet("config")]
    public IActionResult Config()
    {
        var o = options.CurrentValue;
        return Ok(new
        {
            qdrantConfigured = o.QdrantConfigured,
            o.Collection,
            sourceDatabase = string.IsNullOrWhiteSpace(o.SourceConnectionString) ? "ConnectionStrings:Default" : "QdrantSync:SourceConnectionString",
            o.BatchSize,
            o.WriteBatchSize,
            o.IncludeColorShades,
            nightly = new { o.Nightly.Enabled, o.Nightly.AtUtc, o.Nightly.Apply, o.Nightly.Insert, o.Nightly.WindowHours },
            email = new { configured = o.Email.Configured, o.Email.SmtpHost, o.Email.SmtpPort, o.Email.From, o.Email.To },
            instance = QdrantSyncWorker.Instance,
        });
    }
}
