using AdminPanelAPI.Bts.Models;
using Npgsql;

namespace AdminPanelAPI.Bts.Services;

/// <summary>
/// The <c>bts</c> schema: one row per space, an activity log and notes on
/// paths. Files and folders themselves are not recorded here; R2 is the
/// source of truth for them.
/// </summary>
public sealed class SpaceStore
{
    private const string SpaceColumns =
        "id, name, notes, quota_bytes, expires_at, revoked_at, created_by, created_at, updated_at, token_sealed";

    private readonly NpgsqlDataSource _db;
    private readonly ILogger<SpaceStore> _logger;

    public SpaceStore(NpgsqlDataSource db, ILogger<SpaceStore> logger)
    {
        _db = db;
        _logger = logger;
    }

    public async Task<Space?> FindByTokenHashAsync(byte[] hash, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand($"SELECT {SpaceColumns} FROM bts.spaces WHERE token_hash = @h");
        cmd.Parameters.AddWithValue("h", hash);
        return await ReadOneAsync(cmd, ct);
    }

    public async Task<Space?> GetAsync(long id, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand($"SELECT {SpaceColumns} FROM bts.spaces WHERE id = @id");
        cmd.Parameters.AddWithValue("id", id);
        return await ReadOneAsync(cmd, ct);
    }

    public async Task<IReadOnlyList<Space>> ListAsync(CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand($"SELECT {SpaceColumns} FROM bts.spaces ORDER BY lower(name), id");
        var spaces = new List<Space>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            spaces.Add(Read(reader));
        return spaces;
    }

    public async Task<Space> CreateAsync(
        string name, string? notes, long? quotaBytes, DateTimeOffset? expiresAt, string createdBy,
        byte[] tokenHash, byte[] tokenSealed, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand(
            "INSERT INTO bts.spaces (name, notes, quota_bytes, expires_at, created_by, token_hash, token_sealed) " +
            $"VALUES (@name, @notes, @quota, @expires, @by, @hash, @sealed) RETURNING {SpaceColumns}");
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("notes", (object?)notes ?? DBNull.Value);
        cmd.Parameters.AddWithValue("quota", (object?)quotaBytes ?? DBNull.Value);
        cmd.Parameters.AddWithValue("expires", (object?)expiresAt?.ToUniversalTime() ?? DBNull.Value);
        cmd.Parameters.AddWithValue("by", createdBy);
        cmd.Parameters.AddWithValue("hash", tokenHash);
        cmd.Parameters.AddWithValue("sealed", tokenSealed);
        return (await ReadOneAsync(cmd, ct))!;
    }

    public async Task<Space?> UpdateAsync(
        long id, string name, string? notes, long? quotaBytes, DateTimeOffset? expiresAt, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand(
            "UPDATE bts.spaces SET name = @name, notes = @notes, quota_bytes = @quota, expires_at = @expires, " +
            $"updated_at = now() WHERE id = @id RETURNING {SpaceColumns}");
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("notes", (object?)notes ?? DBNull.Value);
        cmd.Parameters.AddWithValue("quota", (object?)quotaBytes ?? DBNull.Value);
        cmd.Parameters.AddWithValue("expires", (object?)expiresAt?.ToUniversalTime() ?? DBNull.Value);
        return await ReadOneAsync(cmd, ct);
    }

    /// <summary>Gives the space a new link, which also lifts a revoke.</summary>
    public async Task<Space?> SetTokenAsync(long id, byte[] tokenHash, byte[] tokenSealed, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand(
            "UPDATE bts.spaces SET token_hash = @hash, token_sealed = @sealed, revoked_at = NULL, updated_at = now() " +
            $"WHERE id = @id RETURNING {SpaceColumns}");
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("hash", tokenHash);
        cmd.Parameters.AddWithValue("sealed", tokenSealed);
        return await ReadOneAsync(cmd, ct);
    }

    public async Task<Space?> RevokeAsync(long id, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand(
            "UPDATE bts.spaces SET revoked_at = COALESCE(revoked_at, now()), updated_at = now() " +
            $"WHERE id = @id RETURNING {SpaceColumns}");
        cmd.Parameters.AddWithValue("id", id);
        return await ReadOneAsync(cmd, ct);
    }

    /// <summary>Best effort: a failed log write never fails the user's action.</summary>
    public async Task LogAsync(
        long spaceId, string actor, string action, string? path, string? newPath, long? bytes, string? ip)
    {
        try
        {
            await using var cmd = _db.CreateCommand(
                "INSERT INTO bts.activity (space_id, actor, action, path, new_path, bytes, ip) " +
                "VALUES (@space, @actor, @action, @path, @new, @bytes, @ip)");
            cmd.Parameters.AddWithValue("space", spaceId);
            cmd.Parameters.AddWithValue("actor", actor);
            cmd.Parameters.AddWithValue("action", action);
            cmd.Parameters.AddWithValue("path", (object?)path ?? DBNull.Value);
            cmd.Parameters.AddWithValue("new", (object?)newPath ?? DBNull.Value);
            cmd.Parameters.AddWithValue("bytes", (object?)bytes ?? DBNull.Value);
            cmd.Parameters.AddWithValue("ip", (object?)ip ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not log {Action} on space {SpaceId}", action, spaceId);
        }
    }

    public async Task<IReadOnlyList<ActivityEntry>> ActivityAsync(long spaceId, int limit, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand(
            "SELECT id, actor, action, path, new_path, bytes, ip, at FROM bts.activity " +
            "WHERE space_id = @space ORDER BY at DESC, id DESC LIMIT @limit");
        cmd.Parameters.AddWithValue("space", spaceId);
        cmd.Parameters.AddWithValue("limit", Math.Clamp(limit, 1, 1000));

        var entries = new List<ActivityEntry>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            entries.Add(new ActivityEntry(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.GetFieldValue<DateTimeOffset>(7)));
        }
        return entries;
    }

    public async Task<Dictionary<string, NoteInfo>> NotesAsync(long spaceId, IReadOnlyCollection<string> paths, CancellationToken ct)
    {
        var notes = new Dictionary<string, NoteInfo>(StringComparer.Ordinal);
        if (paths.Count == 0) return notes;

        await using var cmd = _db.CreateCommand(
            "SELECT path, note, updated_by, updated_at FROM bts.notes WHERE space_id = @space AND path = ANY(@paths)");
        cmd.Parameters.AddWithValue("space", spaceId);
        cmd.Parameters.AddWithValue("paths", paths.ToArray());
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            notes[reader.GetString(0)] = new NoteInfo(reader.GetString(1), reader.GetString(2), reader.GetFieldValue<DateTimeOffset>(3));
        return notes;
    }

    /// <summary>Saves the note on a path, or removes it when <paramref name="text"/> is null.</summary>
    public async Task<NoteInfo?> SetNoteAsync(long spaceId, string path, string? text, string by, CancellationToken ct)
    {
        if (text is null)
        {
            await using var del = _db.CreateCommand("DELETE FROM bts.notes WHERE space_id = @space AND path = @path");
            del.Parameters.AddWithValue("space", spaceId);
            del.Parameters.AddWithValue("path", path);
            await del.ExecuteNonQueryAsync(ct);
            return null;
        }

        await using var cmd = _db.CreateCommand(
            "INSERT INTO bts.notes (space_id, path, note, updated_by) VALUES (@space, @path, @note, @by) " +
            "ON CONFLICT (space_id, path) DO UPDATE SET note = EXCLUDED.note, updated_by = EXCLUDED.updated_by, updated_at = now() " +
            "RETURNING note, updated_by, updated_at");
        cmd.Parameters.AddWithValue("space", spaceId);
        cmd.Parameters.AddWithValue("path", path);
        cmd.Parameters.AddWithValue("note", text);
        cmd.Parameters.AddWithValue("by", by);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);
        return new NoteInfo(reader.GetString(0), reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2));
    }

    /// <summary>
    /// Re-keys notes after a file or folder moved. For a folder (paths ending
    /// in '/') every note underneath goes with it. Stale notes already at the
    /// target are dropped first.
    /// </summary>
    public async Task MoveNotesAsync(long spaceId, string source, string target, bool isFolder, CancellationToken ct)
    {
        var match = isFolder ? "left(path, length(@{0})) = @{0}" : "path = @{0}";
        await using var batch = _db.CreateBatch();
        var drop = new NpgsqlBatchCommand($"DELETE FROM bts.notes WHERE space_id = @space AND {string.Format(match, "target")}");
        var move = new NpgsqlBatchCommand(
            $"UPDATE bts.notes SET path = @target || substr(path, length(@source) + 1) WHERE space_id = @space AND {string.Format(match, "source")}");
        foreach (var c in new[] { drop, move })
        {
            c.Parameters.AddWithValue("space", spaceId);
            c.Parameters.AddWithValue("source", source);
            c.Parameters.AddWithValue("target", target);
            batch.BatchCommands.Add(c);
        }
        await batch.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A member of the shared camera movement roster, which admin sign-in reuses.</summary>
    public async Task<AdminUser?> FindRosterUserAsync(string name, CancellationToken ct)
    {
        await using var cmd = _db.CreateCommand(
            "SELECT name, is_admin, password_hash FROM frl.frl_camera_movement_users " +
            "WHERE lower(name) = lower(@name) LIMIT 1");
        cmd.Parameters.AddWithValue("name", name);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new AdminUser(
            reader.GetString(0), reader.GetBoolean(1), reader.IsDBNull(2) ? null : reader.GetString(2));
    }

    public async Task<bool> PingAsync(CancellationToken ct)
    {
        try
        {
            await using var cmd = _db.CreateCommand("SELECT 1");
            await cmd.ExecuteScalarAsync(ct);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    private static async Task<Space?> ReadOneAsync(NpgsqlCommand cmd, CancellationToken ct)
    {
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Read(reader) : null;
    }

    private static Space Read(NpgsqlDataReader r) => new(
        r.GetInt64(0),
        r.GetString(1),
        r.IsDBNull(2) ? null : r.GetString(2),
        r.IsDBNull(3) ? null : r.GetInt64(3),
        r.IsDBNull(4) ? null : r.GetFieldValue<DateTimeOffset>(4),
        r.IsDBNull(5) ? null : r.GetFieldValue<DateTimeOffset>(5),
        r.GetString(6),
        r.GetFieldValue<DateTimeOffset>(7),
        r.GetFieldValue<DateTimeOffset>(8),
        r.GetFieldValue<byte[]>(9));
}
