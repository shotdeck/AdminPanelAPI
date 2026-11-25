using Npgsql;
using Microsoft.Extensions.DependencyInjection;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Owns the request's lazily opened connection so the scope returns it to
    /// the pool. Lazy&lt;T&gt; is not disposable, so a connection opened through
    /// it and never resolved as NpgsqlConnection was otherwise held until the
    /// pool ran dry.
    /// </summary>
    public sealed class ScopedConnection : IDisposable, IAsyncDisposable
    {
        private readonly Func<NpgsqlConnection> _open;
        private NpgsqlConnection? _connection;

        public ScopedConnection(Func<NpgsqlConnection> open) => _open = open;

        public NpgsqlConnection Get() => _connection ??= _open();

        public void Dispose()
        {
            _connection?.Dispose();
            _connection = null;
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection is not null) await _connection.DisposeAsync();
            _connection = null;
        }
    }

    public static class ScopedConnectionRegistration
    {
        /// <summary>
        /// One connection per scope, opened on first use and returned to the
        /// pool when the scope ends, whether it is taken as NpgsqlConnection
        /// or as Lazy&lt;NpgsqlConnection&gt;.
        /// </summary>
        public static IServiceCollection AddScopedConnection(
            this IServiceCollection services, Func<string> connectionString)
        {
            services.AddScoped(_ => new ScopedConnection(() =>
            {
                var conn = new NpgsqlConnection(connectionString());
                conn.Open();
                return conn;
            }));
            services.AddScoped(sp => new Lazy<NpgsqlConnection>(sp.GetRequiredService<ScopedConnection>().Get));
            services.AddScoped(sp => sp.GetRequiredService<Lazy<NpgsqlConnection>>().Value);
            return services;
        }
    }
}
