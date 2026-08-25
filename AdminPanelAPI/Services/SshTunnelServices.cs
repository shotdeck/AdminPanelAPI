using Npgsql;
using Renci.SshNet;
using ConnectionInfo = Renci.SshNet.ConnectionInfo;

/// <summary>
/// A point-in-time view of the tunnel, so an operator can tell whether the
/// database is unreachable because the tunnel never came up, dropped, or is
/// misconfigured — without reading the container's stdout.
/// </summary>
public sealed record SshTunnelStatus(
    bool Connected,
    bool UsingExistingForward,
    string KeySource,
    bool KeyAvailable,
    string Endpoint,
    int Attempts,
    DateTimeOffset? ConnectedSince,
    DateTimeOffset? LastErrorAt,
    string? LastError);

public class SshTunnelService : IHostedService, IDisposable
{
    private const int MaxBackoffMs = 60_000;

    private SshClient? _sshClient;
    private ForwardedPortLocal? _portForward;
    private readonly string? _sshPrivateKey;
    private readonly IConfiguration _configuration;
    private readonly ILogger<SshTunnelService> _logger;

    // background supervision
    private readonly CancellationTokenSource _cts = new();
    private Task? _loopTask;

    // observable state
    private volatile bool _connected;
    private volatile bool _usingExistingForward;
    private volatile string _keySource = "unknown";
    private volatile bool _keyAvailable;
    private volatile string _endpoint = "";
    private volatile string? _lastError;
    private int _attempts;
    private DateTimeOffset? _connectedSince;
    private DateTimeOffset? _lastErrorAt;

    public Task TunnelReady => _tunnelReady.Task;
    private readonly TaskCompletionSource _tunnelReady = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SshTunnelService(IConfiguration configuration, ILogger<SshTunnelService> logger)
    {
        _sshPrivateKey = configuration["SSH_PRIVATE_KEY"]; // base64 PEM in App Settings
        _configuration = configuration;
        _logger = logger;
    }

    public SshTunnelStatus Status => new(
        _connected,
        _usingExistingForward,
        _keySource,
        _keyAvailable,
        _endpoint,
        Volatile.Read(ref _attempts),
        _connectedSince,
        _lastErrorAt,
        _lastError);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Run a supervise/reconnect loop so the tunnel comes back if it drops
        _loopTask = Task.Run(() => RunLoopAsync(_cts.Token));
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _cts.Cancel();
        try { if (_loopTask is not null) await _loopTask; } catch { /* ignore */ }
        TearDown();
    }

    public void Dispose()
    {
        _cts.Cancel();
        TearDown();
        _cts.Dispose();
    }

    private async Task RunLoopAsync(CancellationToken ct)
    {
        var sshHost = _configuration["SshTunnel:SshHost"];
        var sshPort = _configuration.GetValue<int>("SshTunnel:SshPort");
        var sshUser = _configuration["SshTunnel:SshUser"];
        var sshKeyPath = _configuration["SshTunnel:SshKeyPath"];

        var remoteDbHost = _configuration["SshTunnel:RemoteDbHost"];
        var remoteDbPort = _configuration.GetValue<uint>("SshTunnel:RemoteDbPort");

        var localBindHost = _configuration["SshTunnel:LocalBindHost"];
        var localBindPort = _configuration.GetValue<uint>("SshTunnel:LocalBindPort");

        var backoffMs = _configuration.GetValue<int>("SshTunnel:ReconnectDelayMs", 2000);
        var delayMs = backoffMs;

        var usingInlineKey = !string.IsNullOrWhiteSpace(_sshPrivateKey);
        _keySource = usingInlineKey ? "SSH_PRIVATE_KEY" : $"file:{sshKeyPath}";
        _keyAvailable = usingInlineKey || (!string.IsNullOrWhiteSpace(sshKeyPath) && File.Exists(sshKeyPath));
        _endpoint = $"{sshUser}@{sshHost}:{sshPort} → {remoteDbHost}:{remoteDbPort} (local {localBindHost}:{localBindPort})";

        _logger.LogInformation(
            "SSH tunnel configured: {Endpoint}; key from {KeySource} (available: {KeyAvailable})",
            _endpoint, _keySource, _keyAvailable);

        var localForward = $"{localBindHost}:{localBindPort}";

        while (!ct.IsCancellationRequested)
        {
            Interlocked.Increment(ref _attempts);
            try
            {
                // Another process in this sandbox may already own the local
                // port — typically the previous worker during an overlapped
                // recycle, which makes our bind fail with "Only one usage of
                // each socket address". If its forward reaches the database,
                // use it and watch until it stops working, rather than looping
                // on a bind that cannot succeed while that process lives.
                if (await LocalForwardReachesDatabaseAsync(ct))
                {
                    _usingExistingForward = true;
                    _connected = true;
                    _connectedSince ??= DateTimeOffset.UtcNow;
                    delayMs = backoffMs;
                    _logger.LogInformation(
                        "Using a forward already listening on {LocalForward} (another process in this instance owns it)",
                        localForward);
                    if (!_tunnelReady.Task.IsCompleted)
                        _tunnelReady.SetResult();

                    while (!ct.IsCancellationRequested && await LocalForwardReachesDatabaseAsync(ct))
                        await Task.Delay(5000, ct);

                    _usingExistingForward = false;
                    _connected = false;
                    _connectedSince = null;
                    if (!ct.IsCancellationRequested)
                        _logger.LogWarning("The existing forward on {LocalForward} stopped working; taking it over", localForward);

                    NpgsqlConnection.ClearAllPools();
                    continue;
                }

                // --- Build key ---
                PrivateKeyFile privateKey;
                if (usingInlineKey)
                {
                    byte[] pemBytes = Convert.FromBase64String(_sshPrivateKey!);
                    using var keyStream = new MemoryStream(pemBytes);
                    privateKey = new PrivateKeyFile(keyStream);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(sshKeyPath) || !File.Exists(sshKeyPath))
                        throw new FileNotFoundException(
                            "SSH_PRIVATE_KEY is not set and the key file is missing, so the tunnel cannot start.",
                            sshKeyPath ?? "(SshTunnel:SshKeyPath unset)");

                    privateKey = new PrivateKeyFile(sshKeyPath);
                }

                // --- Connect SSH with keep-alive + timeout ---
                var connectionInfo = new ConnectionInfo(
                    sshHost,
                    sshPort,
                    sshUser,
                    new PrivateKeyAuthenticationMethod(sshUser, privateKey)
                )
                {
                    Timeout = TimeSpan.FromSeconds(20) // connection attempt timeout
                };

                _sshClient = new SshClient(connectionInfo)
                {
                    // send SSH keep-alives so NAT/SNAT doesn't drop the idle session
                    KeepAliveInterval = TimeSpan.FromSeconds(30)
                };

                _sshClient.ErrorOccurred += (_, e) =>
                {
                    RecordError(e.Exception);
                    _logger.LogWarning(e.Exception, "SSH error on the tunnel session");
                };

                _sshClient.Connect();

                // --- Start local forward ---
                _portForward = new ForwardedPortLocal(localBindHost, localBindPort, remoteDbHost, remoteDbPort);
                _portForward.Exception += (_, e) =>
                {
                    RecordError(e.Exception);
                    _logger.LogWarning(e.Exception, "Port forward exception");
                };

                _sshClient.AddForwardedPort(_portForward);
                _portForward.Start();

                // Sockets pooled through the previous forward are dead once it is
                // torn down, so drop them rather than handing them to a request.
                NpgsqlConnection.ClearAllPools();

                _connected = true;
                _connectedSince = DateTimeOffset.UtcNow;
                delayMs = backoffMs;
                _logger.LogInformation("SSH tunnel established: {Endpoint}", _endpoint);
                if (!_tunnelReady.Task.IsCompleted)
                    _tunnelReady.SetResult(); // signal readiness once

                // --- Supervise: stay here until dropped or cancelled ---
                while (!ct.IsCancellationRequested && _sshClient.IsConnected && _portForward.IsStarted)
                {
                    await Task.Delay(1000, ct);
                }

                if (!ct.IsCancellationRequested)
                    _logger.LogWarning("SSH tunnel no longer active; reconnecting");
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                RecordError(ex);
                _logger.LogError(ex, "SSH tunnel could not be established ({KeySource})", _keySource);
                if (!_tunnelReady.Task.IsCompleted)
                    _tunnelReady.SetException(ex);
            }
            finally
            {
                _connected = false;
                _usingExistingForward = false;
                _connectedSince = null;
                TearDown();
            }

            if (ct.IsCancellationRequested) break;

            // Back off so a rejecting or rate-limiting SSH host isn't hammered
            // (repeated fast retries can get the whole instance banned).
            try { await Task.Delay(delayMs, ct); } catch (OperationCanceledException) { break; }
            delayMs = Math.Min(delayMs * 2, MaxBackoffMs);
        }
    }

    /// <summary>
    /// Whether the configured local port already has a listener that reaches
    /// Postgres. A plain TCP check is not enough: the port could be held by an
    /// unrelated listener or a forward whose SSH session has died.
    /// </summary>
    private async Task<bool> LocalForwardReachesDatabaseAsync(CancellationToken ct)
    {
        var connStr = _configuration["ConnectionStrings:Default"];
        if (string.IsNullOrWhiteSpace(connStr)) return false;

        try
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand("SELECT 1;", conn);
            await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            return false;
        }
    }

    private void RecordError(Exception? ex)
    {
        if (ex is null) return;
        _lastError = $"{ex.GetType().Name}: {ex.Message}";
        _lastErrorAt = DateTimeOffset.UtcNow;
    }

    private void TearDown()
    {
        try { _portForward?.Stop(); } catch { }
        try { _sshClient?.Disconnect(); } catch { }
        try { _portForward?.Dispose(); } catch { }
        try { _sshClient?.Dispose(); } catch { }
        _portForward = null;
        _sshClient = null;
    }
}
