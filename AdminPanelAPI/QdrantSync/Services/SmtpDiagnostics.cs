using System.Diagnostics;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace AdminPanelAPI.QdrantSync.Services;

/// <summary>
/// Walks the SMTP handshake (connect → banner → EHLO → STARTTLS/TLS → EHLO) without
/// authenticating or sending mail, to show where delivery from this host breaks.
/// </summary>
public static class SmtpDiagnostics
{
    public sealed record Step(string Name, bool Ok, string Detail, long Ms);

    private static readonly TimeSpan StepTimeout = TimeSpan.FromSeconds(15);

    public static async Task<object> RunAsync(string host, IEnumerable<int> ports, CancellationToken ct)
    {
        string[] addresses;
        try { addresses = (await Dns.GetHostAddressesAsync(host, ct)).Select(a => a.ToString()).ToArray(); }
        catch (Exception ex) { addresses = new[] { "DNS failed: " + Describe(ex) }; }

        var results = new Dictionary<string, List<Step>>();
        foreach (var port in ports)
            results[port.ToString()] = await ProbeAsync(host, port, implicitTls: port == 465, ct);
        return new { host, addresses, ports = results };
    }

    private static async Task<List<Step>> ProbeAsync(string host, int port, bool implicitTls, CancellationToken ct)
    {
        var steps = new List<Step>();
        using var tcp = new TcpClient();
        Stream stream;
        if (!await RunStepAsync(steps, "connect", async t => { await tcp.ConnectAsync(host, port, t); return $"connected to {tcp.Client.RemoteEndPoint}"; }, ct))
            return steps;
        stream = tcp.GetStream();

        if (implicitTls && !await TlsAsync(steps, host, () => stream, s => stream = s, ct)) return steps;
        if (!await RunStepAsync(steps, "banner", t => ReadReplyAsync(stream, "220", t), ct)) return steps;
        if (!await RunStepAsync(steps, "ehlo", t => CommandAsync(stream, "EHLO adminpanelapi", "250", t), ct)) return steps;
        if (!implicitTls)
        {
            if (!await RunStepAsync(steps, "starttls", t => CommandAsync(stream, "STARTTLS", "220", t), ct)) return steps;
            if (!await TlsAsync(steps, host, () => stream, s => stream = s, ct)) return steps;
            if (!await RunStepAsync(steps, "ehlo-after-tls", t => CommandAsync(stream, "EHLO adminpanelapi", "250", t), ct)) return steps;
        }
        try { await CommandAsync(stream, "QUIT", "221", ct); } catch { /* best effort */ }
        return steps;
    }

    private static Task<bool> TlsAsync(List<Step> steps, string host, Func<Stream> get, Action<Stream> set, CancellationToken ct) =>
        RunStepAsync(steps, "tls", async t =>
        {
            var ssl = new SslStream(get(), leaveInnerStreamOpen: false);
            await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions { TargetHost = host }, t);
            set(ssl);
            return $"{ssl.SslProtocol}, cert {ssl.RemoteCertificate?.Subject} issued by {ssl.RemoteCertificate?.Issuer}";
        }, ct);

    private static async Task<bool> RunStepAsync(List<Step> steps, string name, Func<CancellationToken, Task<string>> action, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(StepTimeout);
        try
        {
            steps.Add(new Step(name, true, await action(cts.Token), sw.ElapsedMilliseconds));
            return true;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            var detail = ex is OperationCanceledException ? $"timed out after {StepTimeout.TotalSeconds:0}s" : Describe(ex);
            steps.Add(new Step(name, false, detail, sw.ElapsedMilliseconds));
            return false;
        }
    }

    private static async Task<string> CommandAsync(Stream stream, string command, string expect, CancellationToken ct)
    {
        await stream.WriteAsync(Encoding.ASCII.GetBytes(command + "\r\n"), ct);
        await stream.FlushAsync(ct);
        return await ReadReplyAsync(stream, expect, ct);
    }

    /// <summary>Reads a (possibly multi-line) SMTP reply and checks its code.</summary>
    private static async Task<string> ReadReplyAsync(Stream stream, string expect, CancellationToken ct)
    {
        var lines = new List<string>();
        while (true)
        {
            var line = await ReadLineAsync(stream, ct);
            if (line == null) throw new IOException(lines.Count == 0 ? "connection closed with no reply" : "connection closed mid-reply: " + string.Join(" | ", lines));
            lines.Add(line);
            if (line.Length < 4 || line[3] != '-') break;
        }
        var reply = string.Join(" | ", lines);
        if (!lines[^1].StartsWith(expect, StringComparison.Ordinal)) throw new IOException($"expected {expect}, got: {reply}");
        return reply;
    }

    private static async Task<string?> ReadLineAsync(Stream stream, CancellationToken ct)
    {
        var sb = new StringBuilder();
        var buffer = new byte[1];
        while (true)
        {
            if (await stream.ReadAsync(buffer, ct) == 0) return sb.Length == 0 ? null : sb.ToString();
            if (buffer[0] == '\n') return sb.ToString().TrimEnd('\r');
            sb.Append((char)buffer[0]);
            if (sb.Length > 4096) throw new IOException("reply line too long: " + sb.ToString(0, 200));
        }
    }

    /// <summary>Message of <paramref name="ex"/> and all inner exceptions, with socket error codes.</summary>
    public static string Describe(Exception ex)
    {
        var parts = new List<string>();
        for (var e = ex; e != null; e = e.InnerException)
            parts.Add(e is SocketException se ? $"{e.GetType().Name}({se.SocketErrorCode}): {e.Message}" : $"{e.GetType().Name}: {e.Message}");
        return string.Join(" -> ", parts);
    }
}
