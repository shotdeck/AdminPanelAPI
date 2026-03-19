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
    [Route("api/admin/synonyms")]
    public sealed class SynonymsAdminController : ControllerBase
    {
        private readonly Lazy<NpgsqlConnection> _lazyConnection;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<SynonymsAdminController> _logger;

        private NpgsqlConnection _connection => _lazyConnection.Value;

        public SynonymsAdminController(
            Lazy<NpgsqlConnection> lazyConnection,
            IServiceScopeFactory scopeFactory,
            ILogger<SynonymsAdminController> logger)
        {
            _lazyConnection = lazyConnection;
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        #region Master Terms CRUD

        /// <summary>
        /// GET /api/admin/synonyms/masters
        /// Returns all master terms from the database.
        /// </summary>
        [HttpGet("masters")]
        [ProducesResponseType(typeof(List<MasterTermDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<MasterTermDto>>> GetAllMasters(CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
SELECT m.id, m.master_term, m.is_included, m.category_id, c.category_name
FROM frl.frl_keywords_synonyms_master m
LEFT JOIN frl.frl_keywords_synonyms_category c ON m.category_id = c.id
ORDER BY m.master_term;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<MasterTermDto>();
                while (await reader.ReadAsync(ct))
                {
                    results.Add(new MasterTermDto
                    {
                        Id = reader.GetInt32(0),
                        MasterTerm = reader.GetString(1),
                        IsIncluded = !reader.IsDBNull(2) && reader.GetBoolean(2),
                        CategoryId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                        CategoryName = reader.IsDBNull(4) ? null : reader.GetString(4)
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
        /// GET /api/admin/synonyms/masters/{id}
        /// Returns a single master term by ID.
        /// </summary>
        [HttpGet("masters/{id:int}")]
        [ProducesResponseType(typeof(MasterTermDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<MasterTermDto>> GetMasterById(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
SELECT m.id, m.master_term, m.is_included, m.category_id, c.category_name
FROM frl.frl_keywords_synonyms_master m
LEFT JOIN frl.frl_keywords_synonyms_category c ON m.category_id = c.id
WHERE m.id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Master term with ID {id} not found." });

                return Ok(new MasterTermDto
                {
                    Id = reader.GetInt32(0),
                    MasterTerm = reader.GetString(1),
                    IsIncluded = !reader.IsDBNull(2) && reader.GetBoolean(2),
                    CategoryId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    CategoryName = reader.IsDBNull(4) ? null : reader.GetString(4)
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST /api/admin/synonyms/masters
        /// Creates a new master term.
        /// </summary>
        [HttpPost("masters")]
        [ProducesResponseType(typeof(MasterTermDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<MasterTermDto>> CreateMaster([FromBody] CreateMasterTermRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.MasterTerm))
                return BadRequest(new { Message = "MasterTerm is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
WITH inserted AS (
    INSERT INTO frl.frl_keywords_synonyms_master (master_term, is_included, category_id)
    VALUES (@master_term, @is_included, @category_id)
    RETURNING id, master_term, is_included, category_id
)
SELECT i.id, i.master_term, i.is_included, i.category_id, c.category_name
FROM inserted i
LEFT JOIN frl.frl_keywords_synonyms_category c ON i.category_id = c.id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@master_term", request.MasterTerm.Trim());
                cmd.Parameters.AddWithValue("@is_included", request.IsIncluded ?? true);
                cmd.Parameters.AddWithValue("@category_id", request.CategoryId.HasValue ? request.CategoryId.Value : DBNull.Value);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return BadRequest(new { Message = "Failed to create master term." });

                var result = new MasterTermDto
                {
                    Id = reader.GetInt32(0),
                    MasterTerm = reader.GetString(1),
                    IsIncluded = !reader.IsDBNull(2) && reader.GetBoolean(2),
                    CategoryId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    CategoryName = reader.IsDBNull(4) ? null : reader.GetString(4)
                };

                return CreatedAtAction(nameof(GetMasterById), new { id = result.Id }, result);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A master term '{request.MasterTerm}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// PUT /api/admin/synonyms/masters/{id}
        /// Updates an existing master term.
        /// </summary>
        [HttpPut("masters/{id:int}")]
        [ProducesResponseType(typeof(MasterTermDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<MasterTermDto>> UpdateMaster(int id, [FromBody] UpdateMasterTermRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.MasterTerm))
                return BadRequest(new { Message = "MasterTerm is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
WITH updated AS (
    UPDATE frl.frl_keywords_synonyms_master
    SET master_term = @master_term, is_included = @is_included, category_id = @category_id
    WHERE id = @id
    RETURNING id, master_term, is_included, category_id
)
SELECT u.id, u.master_term, u.is_included, u.category_id, c.category_name
FROM updated u
LEFT JOIN frl.frl_keywords_synonyms_category c ON u.category_id = c.id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@master_term", request.MasterTerm.Trim());
                cmd.Parameters.AddWithValue("@is_included", request.IsIncluded ?? true);
                cmd.Parameters.AddWithValue("@category_id", request.CategoryId.HasValue ? request.CategoryId.Value : DBNull.Value);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Master term with ID {id} not found." });

                return Ok(new MasterTermDto
                {
                    Id = reader.GetInt32(0),
                    MasterTerm = reader.GetString(1),
                    IsIncluded = !reader.IsDBNull(2) && reader.GetBoolean(2),
                    CategoryId = reader.IsDBNull(3) ? null : reader.GetInt32(3),
                    CategoryName = reader.IsDBNull(4) ? null : reader.GetString(4)
                });
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A master term '{request.MasterTerm}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// DELETE /api/admin/synonyms/masters/{id}
        /// Deletes a master term by ID (cascades to synonyms).
        /// </summary>
        [HttpDelete("masters/{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> DeleteMaster(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"DELETE FROM frl.frl_keywords_synonyms_master WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);

                var rowsAffected = await cmd.ExecuteNonQueryAsync(ct);

                if (rowsAffected == 0)
                    return NotFound(new { Message = $"Master term with ID {id} not found." });

                return NoContent();
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        #endregion

        #region Categories CRUD

        /// <summary>
        /// GET /api/admin/synonyms/categories
        /// Returns all categories.
        /// </summary>
        [HttpGet("categories")]
        [ProducesResponseType(typeof(List<CategoryDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<CategoryDto>>> GetAllCategories(CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"SELECT id, category_name FROM frl.frl_keywords_synonyms_category ORDER BY category_name;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<CategoryDto>();
                while (await reader.ReadAsync(ct))
                {
                    results.Add(new CategoryDto
                    {
                        Id = reader.GetInt32(0),
                        CategoryName = reader.GetString(1)
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
        /// GET /api/admin/synonyms/categories/{id}
        /// Returns a single category by ID.
        /// </summary>
        [HttpGet("categories/{id:int}")]
        [ProducesResponseType(typeof(CategoryDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<CategoryDto>> GetCategoryById(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"SELECT id, category_name FROM frl.frl_keywords_synonyms_category WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Category with ID {id} not found." });

                return Ok(new CategoryDto
                {
                    Id = reader.GetInt32(0),
                    CategoryName = reader.GetString(1)
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST /api/admin/synonyms/categories
        /// Creates a new category.
        /// </summary>
        [HttpPost("categories")]
        [ProducesResponseType(typeof(CategoryDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<CategoryDto>> CreateCategory([FromBody] CreateCategoryRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.CategoryName))
                return BadRequest(new { Message = "CategoryName is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
INSERT INTO frl.frl_keywords_synonyms_category (category_name)
VALUES (@category_name)
RETURNING id, category_name;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@category_name", request.CategoryName.Trim());

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return BadRequest(new { Message = "Failed to create category." });

                var result = new CategoryDto
                {
                    Id = reader.GetInt32(0),
                    CategoryName = reader.GetString(1)
                };

                return CreatedAtAction(nameof(GetCategoryById), new { id = result.Id }, result);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A category '{request.CategoryName}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// PUT /api/admin/synonyms/categories/{id}
        /// Updates an existing category.
        /// </summary>
        [HttpPut("categories/{id:int}")]
        [ProducesResponseType(typeof(CategoryDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<CategoryDto>> UpdateCategory(int id, [FromBody] UpdateCategoryRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.CategoryName))
                return BadRequest(new { Message = "CategoryName is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
UPDATE frl.frl_keywords_synonyms_category
SET category_name = @category_name
WHERE id = @id
RETURNING id, category_name;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@category_name", request.CategoryName.Trim());

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Category with ID {id} not found." });

                return Ok(new CategoryDto
                {
                    Id = reader.GetInt32(0),
                    CategoryName = reader.GetString(1)
                });
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A category '{request.CategoryName}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// DELETE /api/admin/synonyms/categories/{id}
        /// Deletes a category by ID.
        /// </summary>
        [HttpDelete("categories/{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> DeleteCategory(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"DELETE FROM frl.frl_keywords_synonyms_category WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);

                var rowsAffected = await cmd.ExecuteNonQueryAsync(ct);

                if (rowsAffected == 0)
                    return NotFound(new { Message = $"Category with ID {id} not found." });

                return NoContent();
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        #endregion

        #region Synonym Terms CRUD

        /// <summary>
        /// GET /api/admin/synonyms/masters/{masterId}/synonyms
        /// Returns all synonyms for a master term.
        /// </summary>
        [HttpGet("masters/{masterId:int}/synonyms")]
        [ProducesResponseType(typeof(List<SynonymDto>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<List<SynonymDto>>> GetSynonymsByMaster(int masterId, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string checkMasterSql = @"SELECT 1 FROM frl.frl_keywords_synonyms_master WHERE id = @master_id;";
                await using (var checkCmd = new NpgsqlCommand(checkMasterSql, _connection))
                {
                    checkCmd.Parameters.AddWithValue("@master_id", masterId);
                    var exists = await checkCmd.ExecuteScalarAsync(ct);
                    if (exists is null)
                        return NotFound(new { Message = $"Master term with ID {masterId} not found." });
                }

                const string sql = @"SELECT id, master_id, synonym_term, is_included FROM frl.frl_keywords_synonyms WHERE master_id = @master_id ORDER BY synonym_term;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@master_id", masterId);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<SynonymDto>();
                while (await reader.ReadAsync(ct))
                {
                    results.Add(new SynonymDto
                    {
                        Id = reader.GetInt32(0),
                        MasterId = reader.GetInt32(1),
                        SynonymTerm = reader.GetString(2),
                        IsIncluded = !reader.IsDBNull(3) && reader.GetBoolean(3)
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
        /// GET /api/admin/synonyms/synonyms/{id}
        /// Returns a single synonym by ID.
        /// </summary>
        [HttpGet("synonyms/{id:int}")]
        [ProducesResponseType(typeof(SynonymDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<SynonymDto>> GetSynonymById(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"SELECT id, master_id, synonym_term, is_included FROM frl.frl_keywords_synonyms WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Synonym with ID {id} not found." });

                return Ok(new SynonymDto
                {
                    Id = reader.GetInt32(0),
                    MasterId = reader.GetInt32(1),
                    SynonymTerm = reader.GetString(2),
                    IsIncluded = !reader.IsDBNull(3) && reader.GetBoolean(3)
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST /api/admin/synonyms/masters/{masterId}/synonyms
        /// Creates a new synonym for a master term.
        /// </summary>
        [HttpPost("masters/{masterId:int}/synonyms")]
        [ProducesResponseType(typeof(SynonymDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<SynonymDto>> CreateSynonym(int masterId, [FromBody] CreateSynonymRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.SynonymTerm))
                return BadRequest(new { Message = "SynonymTerm is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string checkMasterSql = @"SELECT 1 FROM frl.frl_keywords_synonyms_master WHERE id = @master_id;";
                await using (var checkCmd = new NpgsqlCommand(checkMasterSql, _connection))
                {
                    checkCmd.Parameters.AddWithValue("@master_id", masterId);
                    var exists = await checkCmd.ExecuteScalarAsync(ct);
                    if (exists is null)
                        return NotFound(new { Message = $"Master term with ID {masterId} not found." });
                }

                const string sql = @"
INSERT INTO frl.frl_keywords_synonyms (master_id, synonym_term, is_included)
VALUES (@master_id, @synonym_term, @is_included)
RETURNING id, master_id, synonym_term, is_included;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@master_id", masterId);
                cmd.Parameters.AddWithValue("@synonym_term", request.SynonymTerm.Trim());
                cmd.Parameters.AddWithValue("@is_included", request.IsIncluded ?? true);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return BadRequest(new { Message = "Failed to create synonym." });

                var result = new SynonymDto
                {
                    Id = reader.GetInt32(0),
                    MasterId = reader.GetInt32(1),
                    SynonymTerm = reader.GetString(2),
                    IsIncluded = !reader.IsDBNull(3) && reader.GetBoolean(3)
                };

                return CreatedAtAction(nameof(GetSynonymById), new { id = result.Id }, result);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A synonym '{request.SynonymTerm}' already exists for this master term." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// PUT /api/admin/synonyms/synonyms/{id}
        /// Updates an existing synonym.
        /// </summary>
        [HttpPut("synonyms/{id:int}")]
        [ProducesResponseType(typeof(SynonymDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<SynonymDto>> UpdateSynonym(int id, [FromBody] UpdateSynonymRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.SynonymTerm))
                return BadRequest(new { Message = "SynonymTerm is required." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"
UPDATE frl.frl_keywords_synonyms
SET synonym_term = @synonym_term, is_included = @is_included
WHERE id = @id
RETURNING id, master_id, synonym_term, is_included;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@synonym_term", request.SynonymTerm.Trim());
                cmd.Parameters.AddWithValue("@is_included", request.IsIncluded ?? true);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Synonym with ID {id} not found." });

                return Ok(new SynonymDto
                {
                    Id = reader.GetInt32(0),
                    MasterId = reader.GetInt32(1),
                    SynonymTerm = reader.GetString(2),
                    IsIncluded = !reader.IsDBNull(3) && reader.GetBoolean(3)
                });
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A synonym '{request.SynonymTerm}' already exists for this master term." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// DELETE /api/admin/synonyms/synonyms/{id}
        /// Deletes a synonym by ID.
        /// </summary>
        [HttpDelete("synonyms/{id:int}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> DeleteSynonym(int id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"DELETE FROM frl.frl_keywords_synonyms WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);

                var rowsAffected = await cmd.ExecuteNonQueryAsync(ct);

                if (rowsAffected == 0)
                    return NotFound(new { Message = $"Synonym with ID {id} not found." });

                return NoContent();
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        #endregion

        #region CSV Import

        /// <summary>
        /// POST /api/admin/synonyms/import-csv
        /// multipart/form-data:
        ///   - file: CSV with columns:
        ///       NOTES (ignored)
        ///       COLIN ADDED TO SITE? (ignored)
        ///       TYPE (ignored)
        ///       MASTER TERM (required)
        ///       ALT TERM 1..ALT TERM N (optional, flexible)
        ///   - dryRun: optional bool
        ///
        /// Behavior:
        ///   - If dryRun=true: parses + counts only (rolls back)
        ///   - If dryRun=false: clears BOTH tables first, then imports everything
        /// </summary>
        [HttpPost("import-csv")]
        [Consumes("multipart/form-data")]
        [ProducesResponseType(typeof(ImportSynonymsResult), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public async Task<ActionResult<ImportSynonymsResult>> ImportCsv(
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
                // Clear tables (child first)
                const string deleteSynSql = @"DELETE FROM frl.frl_keywords_synonyms;";
                const string deleteMasterSql = @"DELETE FROM frl.frl_keywords_synonyms_master;";

                if (!dryRun)
                {
                    await using (var c1 = new NpgsqlCommand(deleteSynSql, _connection, tx))
                        await c1.ExecuteNonQueryAsync(ct);

                    await using (var c2 = new NpgsqlCommand(deleteMasterSql, _connection, tx))
                        await c2.ExecuteNonQueryAsync(ct);
                }

                // Upsert category and return its ID
                const string upsertCategorySql = @"
INSERT INTO frl.frl_keywords_synonyms_category (category_name)
VALUES (@category_name)
ON CONFLICT (category_name)
DO UPDATE SET category_name = EXCLUDED.category_name
RETURNING id;";

                const string upsertMasterSql = @"
INSERT INTO frl.frl_keywords_synonyms_master (master_term, category_id)
VALUES (@master_term, @category_id)
ON CONFLICT (master_term)
DO UPDATE SET master_term = EXCLUDED.master_term, category_id = EXCLUDED.category_id
RETURNING id;";

                const string insertSynSql = @"
INSERT INTO frl.frl_keywords_synonyms (master_id, synonym_term, is_included)
VALUES (@master_id, @synonym_term, TRUE)
ON CONFLICT (master_id, synonym_term) DO NOTHING;";

                await using var upsertCategoryCmd = new NpgsqlCommand(upsertCategorySql, _connection, tx);
                upsertCategoryCmd.Parameters.Add("@category_name", NpgsqlDbType.Text);

                await using var upsertMasterCmd = new NpgsqlCommand(upsertMasterSql, _connection, tx);
                upsertMasterCmd.Parameters.Add("@master_term", NpgsqlDbType.Text);
                upsertMasterCmd.Parameters.Add("@category_id", NpgsqlDbType.Integer);

                await using var insertSynCmd = new NpgsqlCommand(insertSynSql, _connection, tx);
                insertSynCmd.Parameters.Add("@master_id", NpgsqlDbType.Integer);
                insertSynCmd.Parameters.Add("@synonym_term", NpgsqlDbType.Text);

                // Cache for category name -> id mapping
                var categoryCache = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                var result = new ImportSynonymsResult();

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

                if (!await csv.ReadAsync())
                    return BadRequest("CSV appears to be empty.");

                csv.ReadHeader();
                var headers = csv.HeaderRecord ?? Array.Empty<string>();

                var masterHeader = FindHeader(headers, "MASTER TERM");
                if (string.IsNullOrWhiteSpace(masterHeader))
                    return BadRequest("CSV must contain a 'MASTER TERM' column.");

                // Find TYPE column for category (optional)
                var typeHeader = FindHeader(headers, "TYPE");

                // Flexible ALT TERM columns (ALT TERM 1..N, ALT TERM 10+ supported)
                var altHeaders = headers
                    .Where(h => !string.IsNullOrWhiteSpace(h))
                    .Select(h => h.Trim())
                    .Where(h => h.StartsWith("ALT TERM", StringComparison.OrdinalIgnoreCase))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                while (await csv.ReadAsync())
                {
                    result.RowsRead++;

                    var master = (SafeGet(csv, masterHeader) ?? "").Trim();
                    if (string.IsNullOrWhiteSpace(master))
                    {
                        result.RowsSkipped++;
                        continue;
                    }

                    // Get category from TYPE column if present
                    var categoryName = !string.IsNullOrWhiteSpace(typeHeader) 
                        ? (SafeGet(csv, typeHeader) ?? "").Trim() 
                        : "";
                    int? categoryId = null;

                    // Get or create category if TYPE is specified
                    if (!string.IsNullOrWhiteSpace(categoryName))
                    {
                        if (categoryCache.TryGetValue(categoryName, out var cachedId))
                        {
                            categoryId = cachedId;
                        }
                        else if (!dryRun)
                        {
                            upsertCategoryCmd.Parameters["@category_name"].Value = categoryName;
                            var catScalar = await upsertCategoryCmd.ExecuteScalarAsync(ct);
                            if (catScalar is not null)
                            {
                                categoryId = Convert.ToInt32(catScalar);
                                categoryCache[categoryName] = categoryId.Value;
                            }
                        }
                        else
                        {
                            // In dry run, just track that we would create this category
                            categoryCache[categoryName] = -1;
                            categoryId = -1;
                        }
                    }

                    var alts = new List<string>();
                    foreach (var h in altHeaders)
                    {
                        var v = (SafeGet(csv, h) ?? "").Trim();
                        if (string.IsNullOrWhiteSpace(v)) continue;
                        if (string.Equals(v, master, StringComparison.OrdinalIgnoreCase)) continue;
                        alts.Add(v);
                    }

                    alts = alts.Distinct(StringComparer.OrdinalIgnoreCase).ToList();

                    result.MastersSeen++;
                    if (alts.Count == 0)
                        result.RowsWithNoAlts++;

                    int masterId;

                    if (dryRun)
                    {
                        result.MastersUpserted++;
                        masterId = -1;
                    }
                    else
                    {
                        upsertMasterCmd.Parameters["@master_term"].Value = master;
                        upsertMasterCmd.Parameters["@category_id"].Value = categoryId.HasValue ? categoryId.Value : DBNull.Value;
                        var scalar = await upsertMasterCmd.ExecuteScalarAsync(ct);

                        if (scalar is null)
                        {
                            result.Errors.Add(new ImportRowError
                            {
                                RowNumber = result.RowsRead,
                                MasterTerm = master,
                                Message = "Upsert master returned null id."
                            });
                            continue;
                        }

                        masterId = Convert.ToInt32(scalar);
                        result.MastersUpserted++;
                    }

                    foreach (var alt in alts)
                    {
                        result.SynonymsSeen++;

                        if (dryRun)
                        {
                            result.SynonymsInserted++;
                            continue;
                        }

                        insertSynCmd.Parameters["@master_id"].Value = masterId;
                        insertSynCmd.Parameters["@synonym_term"].Value = alt;

                        var rows = await insertSynCmd.ExecuteNonQueryAsync(ct);
                        if (rows == 1) result.SynonymsInserted++;
                        else result.SynonymsAlreadyExisted++;
                    }
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
                _logger.LogError(ex, "Synonyms CSV import failed.");
                throw;
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        #endregion

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

        public sealed class CategoryDto
        {
            public int Id { get; set; }
            public string CategoryName { get; set; } = default!;
        }

        public sealed class CreateCategoryRequest
        {
            public string? CategoryName { get; set; }
        }

        public sealed class UpdateCategoryRequest
        {
            public string? CategoryName { get; set; }
        }

        public sealed class MasterTermDto
        {
            public int Id { get; set; }
            public string MasterTerm { get; set; } = default!;
            public bool IsIncluded { get; set; }
            public int? CategoryId { get; set; }
            public string? CategoryName { get; set; }
        }

        public sealed class CreateMasterTermRequest
        {
            public string? MasterTerm { get; set; }
            public bool? IsIncluded { get; set; }
            public int? CategoryId { get; set; }
        }

        public sealed class UpdateMasterTermRequest
        {
            public string? MasterTerm { get; set; }
            public bool? IsIncluded { get; set; }
            public int? CategoryId { get; set; }
        }

        public sealed class SynonymDto
        {
            public int Id { get; set; }
            public int MasterId { get; set; }
            public string SynonymTerm { get; set; } = default!;
            public bool IsIncluded { get; set; }
        }

        public sealed class CreateSynonymRequest
        {
            public string? SynonymTerm { get; set; }
            public bool? IsIncluded { get; set; }
        }

        public sealed class UpdateSynonymRequest
        {
            public string? SynonymTerm { get; set; }
            public bool? IsIncluded { get; set; }
        }

        public sealed class ImportCsvRequest
        {
            public IFormFile? File { get; set; }
            public bool? DryRun { get; set; }
        }

        public sealed class ImportSynonymsResult
        {
            public bool DryRun { get; set; }

            public int RowsRead { get; set; }
            public int RowsSkipped { get; set; }
            public int RowsWithNoAlts { get; set; }

            public int MastersSeen { get; set; }
            public int MastersUpserted { get; set; }

            public int SynonymsSeen { get; set; }
            public int SynonymsInserted { get; set; }
            public int SynonymsAlreadyExisted { get; set; }

            public List<ImportRowError> Errors { get; set; } = new();
        }

        public sealed class ImportRowError
        {
            public int RowNumber { get; set; }
            public string? MasterTerm { get; set; }
            public string Message { get; set; } = default!;
        }

        #endregion
    }
}
