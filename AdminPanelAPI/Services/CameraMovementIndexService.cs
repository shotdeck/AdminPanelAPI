using Npgsql;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// Builds the indexes the camera-movement claim query needs (mirrors
    /// migrations/041) once at startup, in the background, so a slot that has
    /// not had migrations run still gets them. Each statement runs on its own
    /// connection command because CREATE INDEX CONCURRENTLY refuses to run
    /// inside a transaction block, and a multi-statement batch is one.
    /// </summary>
    public sealed class CameraMovementIndexService : BackgroundService
    {
        private static readonly (string Name, string Create)[] Indexes =
        {
            ("idx_sb_movieid_filename",
             @"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sb_movieid_filename
    ON frl.frl_image_scene_boundaries (movieid, filename);"),
            ("idx_images_live_weighted_score",
             @"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_images_live_weighted_score
    ON frl.frl_images (weighted_score DESC)
    WHERE status = 'live';"),
        };

        // An interrupted CONCURRENTLY build leaves an INVALID index behind, and
        // IF NOT EXISTS would then skip it forever; drop those first.
        private const string InvalidIndexSql = @"
SELECT c.relname
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'frl' AND c.relname = ANY(@names) AND NOT i.indisvalid;";

        private const int MaxAttempts = 5;
        private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(1);

        private readonly IServiceScopeFactory _scopeFactory;
        private readonly SshTunnelService _tunnel;
        private readonly ILogger<CameraMovementIndexService> _logger;

        public CameraMovementIndexService(
            IServiceScopeFactory scopeFactory,
            SshTunnelService tunnel,
            ILogger<CameraMovementIndexService> logger)
        {
            _scopeFactory = scopeFactory;
            _tunnel = tunnel;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await _tunnel.TunnelReady.WaitAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "SSH tunnel never became ready; still attempting to build camera-movement indexes");
            }

            for (var attempt = 1; attempt <= MaxAttempts && !stoppingToken.IsCancellationRequested; attempt++)
            {
                try
                {
                    await BuildAsync(stoppingToken);
                    return;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Camera-movement index build failed (attempt {Attempt}/{Max})", attempt, MaxAttempts);
                    if (attempt < MaxAttempts)
                        await Task.Delay(RetryDelay, stoppingToken);
                }
            }
        }

        private async Task BuildAsync(CancellationToken ct)
        {
            using var scope = _scopeFactory.CreateScope();
            var connection = scope.ServiceProvider.GetRequiredService<NpgsqlConnection>();

            var invalid = new List<string>();
            await using (var check = new NpgsqlCommand(InvalidIndexSql, connection))
            {
                check.Parameters.AddWithValue("@names", Indexes.Select(x => x.Name).ToArray());
                await using var reader = await check.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    invalid.Add(reader.GetString(0));
            }

            foreach (var name in invalid)
            {
                _logger.LogWarning("Dropping invalid camera-movement index {Index} left by an interrupted build", name);
                await using var drop = new NpgsqlCommand($"DROP INDEX CONCURRENTLY IF EXISTS frl.{name};", connection);
                drop.CommandTimeout = 0;
                await drop.ExecuteNonQueryAsync(ct);
            }

            foreach (var (name, create) in Indexes)
            {
                await using var cmd = new NpgsqlCommand(create, connection);
                cmd.CommandTimeout = 0; // building on millions of rows takes minutes
                var started = DateTimeOffset.UtcNow;
                await cmd.ExecuteNonQueryAsync(ct);
                _logger.LogInformation(
                    "Camera-movement index {Index} ready ({Elapsed})", name, DateTimeOffset.UtcNow - started);
            }
        }
    }
}
