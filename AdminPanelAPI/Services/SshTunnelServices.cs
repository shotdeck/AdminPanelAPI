using Renci.SshNet;
using ConnectionInfo = Renci.SshNet.ConnectionInfo;

public class SshTunnelService : IHostedService, IDisposable
{
    private SshClient? _sshClient;
    private ForwardedPortLocal? _portForward;
    private readonly string _sshPrivateKey;
    private readonly IConfiguration _configuration;

    // background supervision
    private readonly CancellationTokenSource _cts = new();
    private Task? _loopTask;

    public Task TunnelReady => _tunnelReady.Task;
    private readonly TaskCompletionSource _tunnelReady = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public SshTunnelService(IConfiguration configuration)
    {
        _sshPrivateKey = configuration["SSH_PRIVATE_KEY"]; // base64 PEM in App Settings
        _configuration = configuration;
    }

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


        while (!ct.IsCancellationRequested)
        {
            try
            {
                // --- Build key (same as your code) ---
                PrivateKeyFile privateKey;
                if (!string.IsNullOrWhiteSpace(_sshPrivateKey))
                {
                    byte[] pemBytes = Convert.FromBase64String(_sshPrivateKey);
                    using var keyStream = new MemoryStream(pemBytes);
                    privateKey = new PrivateKeyFile(keyStream);
                }
                else
                {
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
                    // **KEY FIX**: send SSH keep-alives so NAT/SNAT doesn’t drop the idle session
                    KeepAliveInterval = TimeSpan.FromSeconds(30)
                };

                _sshClient.ErrorOccurred += (_, e) =>
                    Console.WriteLine("SSH error: " + e.Exception?.Message);

                _sshClient.Connect();

                // --- Start local forward ---
                _portForward = new ForwardedPortLocal(localBindHost, localBindPort, remoteDbHost, remoteDbPort);
                _portForward.Exception += (_, e) =>
                    Console.WriteLine("Port forward exception: " + e.Exception?.Message);

                _sshClient.AddForwardedPort(_portForward);
                _portForward.Start();

                Console.WriteLine($"✅ SSH tunnel established on {localBindHost}:{localBindPort} → {remoteDbHost}:{remoteDbPort}");
                if (!_tunnelReady.Task.IsCompleted)
                    _tunnelReady.SetResult(); // signal readiness once

                // --- Supervise: stay here until dropped or cancelled ---
                while (!ct.IsCancellationRequested && _sshClient.IsConnected && _portForward.IsStarted)
                {
                    await Task.Delay(1000, ct);
                }

                Console.WriteLine("⚠️ Tunnel no longer active; will reconnect…");
            }
            catch (OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception ex)
            {
                Console.WriteLine("❌ Tunnel loop error: " + ex.Message);
                if (!_tunnelReady.Task.IsCompleted)
                    _tunnelReady.SetException(ex);
            }
            finally
            {
                TearDown();
            }

            // small backoff before retry
            if (!ct.IsCancellationRequested)
                await Task.Delay(backoffMs, ct);
        }
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
