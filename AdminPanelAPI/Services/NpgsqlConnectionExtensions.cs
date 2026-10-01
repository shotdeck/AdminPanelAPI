using Npgsql;
using System.Data;

namespace AdminPanelAPI.Services
{
    public static class NpgsqlConnectionExtensions
    {
        /// <summary>
        /// Open the connection unless it is open already. The scoped
        /// NpgsqlConnection comes out of the container open, and Npgsql throws
        /// on a second open, so a background pass that opens it blindly dies
        /// before it does any work.
        /// </summary>
        public static async Task EnsureOpenAsync(
            this NpgsqlConnection connection, CancellationToken ct)
        {
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync(ct);
        }
    }
}
