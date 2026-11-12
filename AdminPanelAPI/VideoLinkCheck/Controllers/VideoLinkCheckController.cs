using System.Text;
using AdminPanelAPI.VideoLinkCheck.Models;
using AdminPanelAPI.VideoLinkCheck.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace AdminPanelAPI.VideoLinkCheck.Controllers;

/// <summary>
/// Checks the YouTube/Vimeo link of every music video and commercial (with 2+ live images) via
/// oEmbed. Runs in the background (<see cref="VideoLinkCheckWorker"/>) and emails the CSVs when done;
/// meant to be called weekly by an Azure Logic App.
/// </summary>
[ApiController]
[Route("api/video-link-check")]
public sealed class VideoLinkCheckController(
    VideoLinkCheckStore store,
    VideoLinkCheckMailer mailer,
    IOptionsMonitor<VideoLinkCheckOptions> options) : ControllerBase
{
    /// <summary>
    /// Queues a check. <paramref name="to"/> (comma-separated) sets who gets this run's email,
    /// otherwise VideoLinkCheck:Email:To. One check runs at a time; 409 with its id if one is going.
    /// </summary>
    [HttpPost("run")]
    public async Task<IActionResult> Run(
        [FromQuery] string? to = null,
        [FromQuery] bool email = true,
        [FromQuery] string triggeredBy = "manual",
        CancellationToken ct = default)
    {
        var recipients = VideoLinkCheckReport.NormalizeRecipients(to, out var invalid);
        if (invalid != null) return BadRequest(new { error = $"Not an email address: {invalid}" });

        await store.EnsureTablesAsync(ct);
        var (jobId, created) = await store.CreateJobAsync(string.IsNullOrWhiteSpace(triggeredBy) ? "manual" : triggeredBy, email, recipients, ct);
        var statusUrl = $"/api/video-link-check/jobs/{jobId}";
        if (!created)
            return Conflict(new { error = "A video link check is already queued or running.", jobId, statusUrl });
        return Accepted(new { jobId, email, to = recipients ?? mailer.Settings.To, statusUrl });
    }

    [HttpGet("jobs")]
    public async Task<ActionResult<List<VideoLinkCheckJob>>> ListJobs([FromQuery] int limit = 20, CancellationToken ct = default)
    {
        await store.EnsureTablesAsync(ct);
        return await store.ListJobsAsync(limit, ct);
    }

    [HttpGet("jobs/{id:long}")]
    public async Task<ActionResult<VideoLinkCheckJob>> GetJob(long id, CancellationToken ct)
    {
        var job = await store.GetJobAsync(id, ct);
        return job == null ? NotFound() : job;
    }

    /// <summary>The job's results as CSV (all movies, or only links that aren't active).</summary>
    [HttpGet("jobs/{id:long}/results.csv")]
    public async Task<IActionResult> ResultsCsv(long id, [FromQuery] bool issuesOnly = false, CancellationToken ct = default)
    {
        var job = await store.GetJobAsync(id, ct);
        if (job == null) return NotFound();
        var csv = VideoLinkCheckReport.Csv(await store.GetResultsAsync(job, issuesOnly, ct));
        var bytes = Encoding.UTF8.GetPreamble().Concat(Encoding.UTF8.GetBytes(csv)).ToArray();
        return File(bytes, "text/csv", VideoLinkCheckReport.FileName(job, issuesOnly));
    }

    /// <summary>Cancels a queued job, or stops a running one after its current batch.</summary>
    [HttpPost("jobs/{id:long}/cancel")]
    public async Task<IActionResult> Cancel(long id, CancellationToken ct) =>
        await store.CancelAsync(id, ct) ? Ok(new { id, status = "cancelled" }) : Conflict(new { error = "Job is not queued or running." });

    /// <summary>Re-queues a failed or cancelled job; it carries on after the last saved movie.</summary>
    [HttpPost("jobs/{id:long}/resume")]
    public async Task<IActionResult> Resume(long id, CancellationToken ct) =>
        await store.ResumeAsync(id, ct)
            ? Accepted(new { id, status = "queued" })
            : Conflict(new { error = "Job is not failed or cancelled, or another check is queued or running." });

    /// <summary>Emails the job's report now, to <paramref name="to"/> if given, else the job's/default recipients.</summary>
    [HttpPost("jobs/{id:long}/email")]
    public async Task<IActionResult> Email(long id, [FromQuery] string? to = null, CancellationToken ct = default)
    {
        var recipients = VideoLinkCheckReport.NormalizeRecipients(to, out var invalid);
        if (invalid != null) return BadRequest(new { error = $"Not an email address: {invalid}" });
        var job = await store.GetJobAsync(id, ct);
        if (job == null) return NotFound();
        var status = await mailer.SendAndRecordAsync(job, recipients, ct);
        return status.StartsWith("sent") ? Ok(new { id, emailStatus = status }) : StatusCode(502, new { id, emailStatus = status });
    }

    /// <summary>Settings in effect (no secrets).</summary>
    [HttpGet("config")]
    public IActionResult Config()
    {
        var o = options.CurrentValue;
        var smtp = mailer.Settings;
        return Ok(new
        {
            mediaTypes = o.MediaTypeList,
            o.MinLiveImages,
            o.BatchSize,
            o.MaxConcurrent,
            o.MaxRetries,
            o.RequestTimeoutSeconds,
            email = new { configured = smtp.Configured, smtp.Host, smtp.Port, smtp.From, defaultTo = smtp.To },
            instance = VideoLinkCheckWorker.Instance,
        });
    }
}
