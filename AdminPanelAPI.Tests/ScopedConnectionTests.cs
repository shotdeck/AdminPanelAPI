using AdminPanelAPI.Services;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace AdminPanelAPI.Tests;

public class ScopedConnectionTests
{
    [Fact]
    public void A_scope_that_never_asks_for_a_connection_opens_none()
    {
        var opened = 0;
        using (new ScopedConnection(() => { opened++; return new NpgsqlConnection(); })) { }
        Assert.Equal(0, opened);
    }

    /// <summary>
    /// Needs a Postgres: set ADMINPANEL_TEST_PG to its connection string.
    /// Ten requests through a two-connection pool only succeed if each scope
    /// hands its connection back.
    /// </summary>
    [Fact]
    public async Task Connections_taken_through_lazy_go_back_to_the_pool()
    {
        var connStr = Environment.GetEnvironmentVariable("ADMINPANEL_TEST_PG");
        if (string.IsNullOrWhiteSpace(connStr)) return;
        connStr = new NpgsqlConnectionStringBuilder(connStr) { MaxPoolSize = 2, Timeout = 3 }.ConnectionString;

        var services = new ServiceCollection().AddScopedConnection(() => connStr);

        await using var provider = services.BuildServiceProvider();
        for (var i = 0; i < 10; i++)
        {
            await using var scope = provider.CreateAsyncScope();
            var conn = i % 2 == 0
                ? scope.ServiceProvider.GetRequiredService<Lazy<NpgsqlConnection>>().Value
                : scope.ServiceProvider.GetRequiredService<NpgsqlConnection>();
            await using var cmd = new NpgsqlCommand("SELECT 1", conn);
            Assert.Equal(1, await cmd.ExecuteScalarAsync());
        }
    }
}
