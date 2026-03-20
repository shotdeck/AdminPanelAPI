using Npgsql;
using System.Data;

namespace ShotDeckSearch.Services;

/// <summary>
/// Runs database migrations on application startup.
/// Ensures the unique constraint on frl_popularity_tag_rules is on (tag, category)
/// instead of just (tag), so the same tag can exist with different categories.
/// </summary>
public sealed class DatabaseMigrationService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<DatabaseMigrationService> _logger;

    public DatabaseMigrationService(
        IServiceProvider serviceProvider,
        ILogger<DatabaseMigrationService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Running database migrations...");

        using var scope = _serviceProvider.CreateScope();
        var connection = scope.ServiceProvider.GetRequiredService<NpgsqlConnection>();

        if (connection.State != ConnectionState.Open)
            await connection.OpenAsync(cancellationToken);

        try
        {
            await MigrateTagPopularityUniqueConstraint(connection, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Database migration failed. The application will continue but duplicate tag checks may not work correctly.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task MigrateTagPopularityUniqueConstraint(NpgsqlConnection connection, CancellationToken ct)
    {
        // Check if the new composite index already exists
        const string checkNewIndexSql = @"
SELECT COUNT(*) FROM pg_indexes
WHERE schemaname = 'frl'
  AND tablename = 'frl_popularity_tag_rules'
  AND indexname = 'uq_frl_popularity_tag_rules_tag_category';";

        await using var checkCmd = new NpgsqlCommand(checkNewIndexSql, connection);
        var newIndexExists = (long)(await checkCmd.ExecuteScalarAsync(ct))! > 0;

        if (newIndexExists)
        {
            _logger.LogInformation("Tag popularity (tag, category) unique index already exists. Skipping migration.");
            return;
        }

        _logger.LogInformation("Migrating tag popularity unique constraint from (tag) to (tag, category)...");

        // Drop old tag-only unique constraints
        const string findOldConstraintsSql = @"
SELECT conname
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname = 'frl'
  AND c.conrelid = 'frl.frl_popularity_tag_rules'::regclass
  AND c.contype = 'u';";

        var constraintsToDrop = new List<string>();
        await using (var findCmd = new NpgsqlCommand(findOldConstraintsSql, connection))
        await using (var reader = await findCmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                constraintsToDrop.Add(reader.GetString(0));
            }
        }

        foreach (var constraintName in constraintsToDrop)
        {
            _logger.LogInformation("Dropping old unique constraint: {ConstraintName}", constraintName);
            var dropSql = $"ALTER TABLE frl.frl_popularity_tag_rules DROP CONSTRAINT \"{constraintName}\";";
            await using var dropCmd = new NpgsqlCommand(dropSql, connection);
            await dropCmd.ExecuteNonQueryAsync(ct);
        }

        // Also drop any unique indexes that aren't constraints (created via CREATE UNIQUE INDEX)
        const string findOldIndexesSql = @"
SELECT indexname
FROM pg_indexes
WHERE schemaname = 'frl'
  AND tablename = 'frl_popularity_tag_rules'
  AND indexdef ILIKE '%unique%'
  AND indexname != 'uq_frl_popularity_tag_rules_tag_category';";

        var indexesToDrop = new List<string>();
        await using (var findIdxCmd = new NpgsqlCommand(findOldIndexesSql, connection))
        await using (var idxReader = await findIdxCmd.ExecuteReaderAsync(ct))
        {
            while (await idxReader.ReadAsync(ct))
            {
                indexesToDrop.Add(idxReader.GetString(0));
            }
        }

        foreach (var indexName in indexesToDrop)
        {
            _logger.LogInformation("Dropping old unique index: {IndexName}", indexName);
            var dropIdxSql = $"DROP INDEX IF EXISTS frl.\"{indexName}\";";
            await using var dropIdxCmd = new NpgsqlCommand(dropIdxSql, connection);
            await dropIdxCmd.ExecuteNonQueryAsync(ct);
        }

        // Create the new composite unique index
        const string createIndexSql = @"
CREATE UNIQUE INDEX uq_frl_popularity_tag_rules_tag_category
ON frl.frl_popularity_tag_rules (tag, COALESCE(category, ''));";

        await using var createCmd = new NpgsqlCommand(createIndexSql, connection);
        await createCmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Successfully migrated unique constraint to (tag, category).");
    }
}
