using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using System.Data;
using System.Globalization;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/unwanted-words")]
    public sealed class UnwantedWordsAdminController : ControllerBase
    {
        private readonly Lazy<NpgsqlConnection> _lazyConnection;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<UnwantedWordsAdminController> _logger;

        private NpgsqlConnection _connection => _lazyConnection.Value;

        public UnwantedWordsAdminController(
            Lazy<NpgsqlConnection> lazyConnection,
            IServiceScopeFactory scopeFactory,
            ILogger<UnwantedWordsAdminController> logger)
        {
            _lazyConnection = lazyConnection;
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        [HttpGet("test")]
        public IActionResult Test()
        {
            // Always returns 200 to verify the app runs
            return Ok(new
            {
                status = "It worked",
                time = DateTime.UtcNow
            });
        }

        /// <summary>
        /// GET /api/admin/unwanted-words
        /// Returns all unwanted words from the database.
        /// </summary>
        [HttpGet]
        [ProducesResponseType(typeof(List<UnwantedWordDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<UnwantedWordDto>>> GetAll(CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"SELECT id, phrase, is_super_blacklist FROM frl.frl_keywords_unwanted_words ORDER BY phrase;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<UnwantedWordDto>();
                while (await reader.ReadAsync(ct))
                {
                    results.Add(new UnwantedWordDto
                    {
                        Id = reader.GetInt32(0),
                        Phrase = reader.GetString(1),
                        IsSuperBlacklist = !reader.IsDBNull(2) && reader.GetBoolean(2)
                    });
                }

                return Ok(results);
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET /api/admin/unwanted-words/{id}
        /// Returns a single unwanted word by ID.
        /// </summary>
        [HttpGet("{id:int}")]
        [ProducesResponseType(typeof(UnwantedWordDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<UnwantedWordDto>> GetById(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"SELECT id, phrase, is_super_blacklist FROM frl.frl_keywords_unwanted_words WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Unwanted word with ID {id} not found." });

                return Ok(new UnwantedWordDto
                {
                    Id = reader.GetInt32(0),
                    Phrase = reader.GetString(1),
                    IsSuperBlacklist = !reader.IsDBNull(2) && reader.GetBoolean(2)
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST /api/admin/unwanted-words
        /// Creates a new unwanted word.
        /// </summary>
        [HttpPost]
        [ProducesResponseType(typeof(UnwantedWordDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<UnwantedWordDto>> Create([FromBody] CreateUnwantedWordRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.Phrase))
                return BadRequest(new { Message = "Phrase is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
INSERT INTO frl.frl_keywords_unwanted_words (phrase, is_super_blacklist)
VALUES (@phrase, @is_super_blacklist)
RETURNING id, phrase, is_super_blacklist;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@phrase", request.Phrase.Trim());
                cmd.Parameters.AddWithValue("@is_super_blacklist", request.IsSuperBlacklist ?? false);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return BadRequest(new { Message = "Failed to create unwanted word." });

                var result = new UnwantedWordDto
                {
                    Id = reader.GetInt32(0),
                    Phrase = reader.GetString(1),
                    IsSuperBlacklist = !reader.IsDBNull(2) && reader.GetBoolean(2)
                };

                return CreatedAtAction(nameof(GetById), new { id = result.Id }, result);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"An unwanted word with phrase '{request.Phrase}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// PUT /api/admin/unwanted-words/{id}
        /// Updates an existing unwanted word.
        /// </summary>
        [HttpPut("{id:int}")]
        [ProducesResponseType(typeof(UnwantedWordDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<UnwantedWordDto>> Update(int id, [FromBody] UpdateUnwantedWordRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.Phrase))
                return BadRequest(new { Message = "Phrase is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
UPDATE frl.frl_keywords_unwanted_words
SET phrase = @phrase, is_super_blacklist = @is_super_blacklist
WHERE id = @id
RETURNING id, phrase, is_super_blacklist;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@phrase", request.Phrase.Trim());
                cmd.Parameters.AddWithValue("@is_super_blacklist", request.IsSuperBlacklist ?? false);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Unwanted word with ID {id} not found." });

                return Ok(new UnwantedWordDto
                {
                    Id = reader.GetInt32(0),
                    Phrase = reader.GetString(1),
                    IsSuperBlacklist = !reader.IsDBNull(2) && reader.GetBoolean(2)
                });
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"An unwanted word with phrase '{request.Phrase}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// DELETE /api/admin/unwanted-words/{id}
        /// Deletes an unwanted word by ID.
        /// </summary>
        [HttpDelete("{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> Delete(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"DELETE FROM frl.frl_keywords_unwanted_words WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);

                var rowsAffected = await cmd.ExecuteNonQueryAsync(ct);

                if (rowsAffected == 0)
                    return NotFound(new { Message = $"Unwanted word with ID {id} not found." });

                return NoContent();
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST /api/admin/unwanted-words/import-csv
        /// multipart/form-data:
        ///   - file: CSV with columns:
        ///       WORD (required) - the unwanted phrase
        ///       SUPER BLACKLIST (optional) - TRUE/FALSE for substring matching
        ///       NOTES (ignored)
        ///   - dryRun: optional bool
        ///
        /// Behavior:
        ///   - If dryRun=true: parses + counts only (rolls back)
        ///   - If dryRun=false: clears table first, then imports everything
        /// </summary>
        [HttpPost("import-csv")]
        [Consumes("multipart/form-data")]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<ImportUnwantedWordsResult>> ImportCsv(
            [FromForm] ImportCsvRequest req,
            CancellationToken ct)
        {
            if (req.File is null || req.File.Length == 0)
                return BadRequest("CSV file is required.");

            var dryRun = req.DryRun ?? false;

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            await using var tx = await _connection.BeginTransactionAsync(ct);

            try
            {
                const string deleteSql = @"DELETE FROM frl.frl_keywords_unwanted_words;";

                if (!dryRun)
                {
                    await using (var c1 = new NpgsqlCommand(deleteSql, _connection, tx))
                        await c1.ExecuteNonQueryAsync(ct);
                }

                const string insertSql = @"
INSERT INTO frl.frl_keywords_unwanted_words (phrase, is_super_blacklist)
VALUES (@phrase, @is_super_blacklist)
ON CONFLICT (phrase) DO UPDATE SET is_super_blacklist = EXCLUDED.is_super_blacklist
RETURNING id;";

                await using var insertCmd = new NpgsqlCommand(insertSql, _connection, tx);
                insertCmd.Parameters.Add("@phrase", NpgsqlDbType.Text);
                insertCmd.Parameters.Add("@is_super_blacklist", NpgsqlDbType.Boolean);

                var result = new ImportUnwantedWordsResult();

                using var reader = new StreamReader(req.File.OpenReadStream());

                var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    TrimOptions = TrimOptions.Trim,
                    IgnoreBlankLines = true,
                    BadDataFound = null,
                    MissingFieldFound = null,
                    HeaderValidated = null,
                    PrepareHeaderForMatch = args => (args.Header ?? "").Trim()
                };

                using var csv = new CsvReader(reader, csvConfig);

                // Skip any leading notes/comments until we find the header row
                string[]? headers = null;
                while (await csv.ReadAsync())
                {
                    csv.ReadHeader();
                    headers = csv.HeaderRecord ?? Array.Empty<string>();
                    
                    // Check if this row contains the WORD header
                    var wordHeader = FindHeader(headers, "WORD");
                    if (!string.IsNullOrWhiteSpace(wordHeader))
                        break;
                }

                if (headers == null || headers.Length == 0)
                    return BadRequest("CSV appears to be empty.");

                var wordHeaderName = FindHeader(headers, "WORD");
                if (string.IsNullOrWhiteSpace(wordHeaderName))
                    return BadRequest("CSV must contain a 'WORD' column.");

                // Find SUPER BLACKLIST column (flexible naming)
                var superBlacklistHeader = headers
                    .FirstOrDefault(h => !string.IsNullOrWhiteSpace(h) &&
                        (h.Trim().StartsWith("SUPER BLACKLIST", StringComparison.OrdinalIgnoreCase) ||
                         h.Trim().Equals("SUPER_BLACKLIST", StringComparison.OrdinalIgnoreCase) ||
                         h.Trim().Equals("IS_SUPER_BLACKLIST", StringComparison.OrdinalIgnoreCase)));

                while (await csv.ReadAsync())
                {
                    result.RowsRead++;

                    var phrase = (SafeGet(csv, wordHeaderName) ?? "").Trim();
                    if (string.IsNullOrWhiteSpace(phrase))
                    {
                        result.RowsSkipped++;
                        continue;
                    }

                    // Parse super blacklist flag
                    var isSuperBlacklist = false;
                    if (!string.IsNullOrWhiteSpace(superBlacklistHeader))
                    {
                        var superBlacklistValue = (SafeGet(csv, superBlacklistHeader) ?? "").Trim();
                        isSuperBlacklist = string.Equals(superBlacklistValue, "TRUE", StringComparison.OrdinalIgnoreCase) ||
                                          string.Equals(superBlacklistValue, "1", StringComparison.OrdinalIgnoreCase) ||
                                          string.Equals(superBlacklistValue, "YES", StringComparison.OrdinalIgnoreCase);
                    }

                    result.PhrasesSeen++;
                    if (isSuperBlacklist)
                        result.SuperBlacklistCount++;

                    if (dryRun)
                    {
                        result.PhrasesInserted++;
                        continue;
                    }

                    insertCmd.Parameters["@phrase"].Value = phrase;
                    insertCmd.Parameters["@is_super_blacklist"].Value = isSuperBlacklist;

                    var scalar = await insertCmd.ExecuteScalarAsync(ct);

                    if (scalar is null)
                    {
                        result.Errors.Add(new ImportRowError
                        {
                            RowNumber = result.RowsRead,
                            Phrase = phrase,
                            Message = "Insert returned null id."
                        });
                        continue;
                    }

                    result.PhrasesInserted++;
                }

                if (dryRun)
                {
                    await tx.RollbackAsync(ct);
                    result.DryRun = true;
                    return Ok(result);
                }

                await tx.CommitAsync(ct);
                return Ok(result);
            }
            catch (Exception ex)
            {
                await tx.RollbackAsync(ct);
                _logger.LogError(ex, "Unwanted words CSV import failed.");
                throw;
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        #region Helpers

        private static string? FindHeader(string[] headers, string wanted)
            => headers.FirstOrDefault(h =>
                string.Equals((h ?? "").Trim(), wanted, StringComparison.OrdinalIgnoreCase));

        private static string? SafeGet(CsvReader csv, string header)
        {
            try { return csv.GetField(header); }
            catch { return null; }
        }

        #endregion

        #region DTOs

        public sealed class UnwantedWordDto
        {
            public int Id { get; set; }
            public string Phrase { get; set; } = default!;
            public bool IsSuperBlacklist { get; set; }
        }

        public sealed class CreateUnwantedWordRequest
        {
            public string? Phrase { get; set; }
            public bool? IsSuperBlacklist { get; set; }
        }

        public sealed class UpdateUnwantedWordRequest
        {
            public string? Phrase { get; set; }
            public bool? IsSuperBlacklist { get; set; }
        }

        public sealed class ImportCsvRequest
        {
            public IFormFile? File { get; set; }
            public bool? DryRun { get; set; }
        }

        public sealed class ImportUnwantedWordsResult
        {
            public bool DryRun { get; set; }

            public int RowsRead { get; set; }
            public int RowsSkipped { get; set; }

            public int PhrasesSeen { get; set; }
            public int PhrasesInserted { get; set; }
            public int SuperBlacklistCount { get; set; }

            public List<ImportRowError> Errors { get; set; } = new();
        }

        public sealed class ImportRowError
        {
            public int RowNumber { get; set; }
            public string? Phrase { get; set; }
            public string Message { get; set; } = default!;
        }

        #endregion
    }
}
