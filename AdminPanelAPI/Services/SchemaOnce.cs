using System.Collections.Concurrent;
using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Runs a controller's create-if-missing schema SQL once per process rather
    /// than on every request. Run concurrently, its ALTER TABLE ... ADD COLUMN IF
    /// NOT EXISTS statements fail each other ("tuple concurrently updated",
    /// deadlocks), which surfaced as random 500s when a page loaded several
    /// tagging requests at once.
    /// </summary>
    public static class SchemaOnce
    {
        private static readonly ConcurrentDictionary<string, bool> Done = new();
        private static readonly SemaphoreSlim Gate = new(1, 1);

        public static async Task EnsureAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
        {
            if (Done.ContainsKey(sql)) return;
            await Gate.WaitAsync(ct);
            try
            {
                if (Done.ContainsKey(sql)) return;
                await using var cmd = new NpgsqlCommand(sql, connection);
                await cmd.ExecuteNonQueryAsync(ct);
                Done[sql] = true;
            }
            finally
            {
                Gate.Release();
            }
        }
    }
}
