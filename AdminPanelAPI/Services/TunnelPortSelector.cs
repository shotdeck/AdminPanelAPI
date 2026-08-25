using System.Net;
using System.Net.Sockets;
using Npgsql;

/// <summary>
/// Picks the loopback port the SSH tunnel forwards from.
/// <para>
/// Deployment slots share the worker's loopback, so two slots configured with
/// the same <c>SshTunnel:LocalBindPort</c> contend for it: whichever starts
/// second cannot bind ("Only one usage of each socket address is normally
/// permitted") and has no route to Postgres. Rather than requiring a distinct
/// port per slot in App Settings, keep the configured port when it is free and
/// fall back to any free port, rewriting the connection string to match so
/// every consumer of <c>ConnectionStrings:Default</c> follows the tunnel.
/// </para>
/// </summary>
public static class TunnelPortSelector
{
    /// <summary>
    /// Resolves the local bind port and writes it, plus the matching
    /// connection string, back into configuration. Call before the host runs:
    /// consumers read <c>ConnectionStrings:Default</c> in their constructors.
    /// </summary>
    public static void Apply(IConfiguration configuration)
    {
        var bindHost = configuration["SshTunnel:LocalBindHost"];
        var configuredPort = configuration.GetValue<int>("SshTunnel:LocalBindPort");
        var connStr = configuration["ConnectionStrings:Default"];

        if (string.IsNullOrWhiteSpace(bindHost) || configuredPort <= 0 || string.IsNullOrWhiteSpace(connStr))
            return;

        if (!IPAddress.TryParse(bindHost, out var bindAddress))
            return;

        if (IsFree(bindAddress, configuredPort))
            return;

        var port = FindFreePort(bindAddress);
        if (port is null || port == configuredPort)
            return;

        var builder = new NpgsqlConnectionStringBuilder(connStr) { Port = port.Value };

        configuration["SshTunnel:LocalBindPort"] = port.Value.ToString();
        configuration["ConnectionStrings:Default"] = builder.ConnectionString;
    }

    private static bool IsFree(IPAddress address, int port)
    {
        try
        {
            using var listener = new TcpListener(address, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }

    private static int? FindFreePort(IPAddress address)
    {
        try
        {
            using var listener = new TcpListener(address, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
        catch (SocketException)
        {
            return null;
        }
    }
}
