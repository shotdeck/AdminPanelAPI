using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using ShotDeck.Keywords;
using System.Data;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/admin/tag-popularity")]
    public sealed class TagPopularityController : ControllerBase
    {
        private readonly NpgsqlConnection _connection;
        private readonly IKeywordCacheService _keywordCache;
        private readonly ILogger<TagPopularityController> _logger;

        public TagPopularityController(
            NpgsqlConnection connection,
            IKeywordCacheService keywordCache,
            ILogger<TagPopularityController> logger)
        {
            _connection = connection;
            _keywordCache = keywordCache;
            _logger = logger;
        }

        [HttpGet]
        [ProducesResponseType(typeof(List<TagPopularityRuleDto>), StatusCodes.Status200OK)]
        public async Task<ActionResult<List<TagPopularityRuleDto>>> GetAll(
    [FromQuery] string? tag,
    CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                string sql;
                await using var cmd = new NpgsqlCommand();
                cmd.Connection = _connection;

                if (string.IsNullOrWhiteSpace(tag))
                {
                    sql = @"
SELECT id, tag, percentage, is_active, created_at, updated_at, category
FROM frl.frl_popularity_tag_rules
ORDER BY tag;";
                }
                else
                {
                    sql = @"
SELECT id, tag, percentage, is_active, created_at, updated_at, category
FROM frl.frl_popularity_tag_rules
WHERE tag ILIKE @tag
ORDER BY tag;";

                    cmd.Parameters.AddWithValue("@tag", $"%{tag.Trim()}%");
                }

                cmd.CommandText = sql;

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                var results = new List<TagPopularityRuleDto>();
                while (await reader.ReadAsync(ct))
                {
                    results.Add(MapToDto(reader));
                }

                return Ok(results);
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET /api/admin/tag-popularity/{id}
        /// Returns a single tag popularity rule by ID.
        /// </summary>
        [HttpGet("{id:long}")]
        [ProducesResponseType(typeof(TagPopularityRuleDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<TagPopularityRuleDto>> GetById(long id, CancellationToken ct)
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
SELECT id, tag, percentage, is_active, created_at, updated_at, category
FROM frl.frl_popularity_tag_rules
WHERE id = @id;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Tag popularity rule with ID {id} not found." });

                return Ok(MapToDto(reader));
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// GET /api/admin/tag-popularity/search?tag={tag}
        /// Searches all cached keywords (case-insensitive partial match)
        /// and returns each result with its origin (e.g. Director, Lighting Type, Shot Type).
        /// </summary>
        [HttpGet("search")]
        [ProducesResponseType(typeof(List<TagWithOrigin>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public ActionResult<List<TagWithOrigin>> SearchByTag([FromQuery] string tag)
        {
            if (string.IsNullOrWhiteSpace(tag))
                return BadRequest(new { Message = "Tag query parameter is required." });

            var results = _keywordCache.SearchAllWithOrigin(tag);
            return Ok(results);
        }

        /// <summary>
        /// POST /api/admin/tag-popularity
        /// Creates a new tag popularity rule.
        /// </summary>
        [HttpPost]
        [ProducesResponseType(typeof(TagPopularityRuleDto), StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<TagPopularityRuleDto>> Create([FromBody] CreateTagPopularityRuleRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.Tag))
                return BadRequest(new { Message = "Tag is required." });

            if (request.Percentage < -100 || request.Percentage > 1000)
                return BadRequest(new { Message = "Percentage must be between -100 and 1000." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                // Check for existing rule with same tag AND category
                const string checkSql = @"
SELECT COUNT(*) FROM frl.frl_popularity_tag_rules
WHERE tag = @tag AND category IS NOT DISTINCT FROM @category;";

                await using (var checkCmd = new NpgsqlCommand(checkSql, _connection))
                {
                    checkCmd.Parameters.AddWithValue("@tag", request.Tag.Trim());
                    checkCmd.Parameters.AddWithValue("@category", (object?)request.Category ?? DBNull.Value);
                    var count = (long)(await checkCmd.ExecuteScalarAsync(ct))!;
                    if (count > 0)
                    {
                        var categoryDisplay = request.Category ?? "(no category)";
                        return Conflict(new { Message = $"A tag popularity rule with tag '{request.Tag}' and category '{categoryDisplay}' already exists." });
                    }
                }

                const string sql = @"
INSERT INTO frl.frl_popularity_tag_rules (tag, percentage, is_active, category)
VALUES (@tag, @percentage, @is_active, @category)
RETURNING id, tag, percentage, is_active, created_at, updated_at, category;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@tag", request.Tag.Trim());
                cmd.Parameters.AddWithValue("@percentage", request.Percentage);
                cmd.Parameters.AddWithValue("@is_active", request.IsActive ?? true);
                cmd.Parameters.AddWithValue("@category", (object?)request.Category ?? DBNull.Value);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return BadRequest(new { Message = "Failed to create tag popularity rule." });

                var result = MapToDto(reader);
                return CreatedAtAction(nameof(GetById), new { id = result.Id }, result);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A tag popularity rule with tag '{request.Tag}' and category '{request.Category ?? "(no category)"}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// PUT /api/admin/tag-popularity/{id}
        /// Updates an existing tag popularity rule.
        /// </summary>
        [HttpPut("{id:long}")]
        [ProducesResponseType(typeof(TagPopularityRuleDto), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<TagPopularityRuleDto>> Update(long id, [FromBody] UpdateTagPopularityRuleRequest request, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(request.Tag))
                return BadRequest(new { Message = "Tag is required." });

            if (request.Percentage < -100 || request.Percentage > 1000)
                return BadRequest(new { Message = "Percentage must be between -100 and 1000." });

            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                // Check for existing rule with same tag AND category (excluding current record)
                const string checkSql = @"
SELECT COUNT(*) FROM frl.frl_popularity_tag_rules
WHERE tag = @tag AND category IS NOT DISTINCT FROM @category AND id != @id;";

                await using (var checkCmd = new NpgsqlCommand(checkSql, _connection))
                {
                    checkCmd.Parameters.AddWithValue("@tag", request.Tag.Trim());
                    checkCmd.Parameters.AddWithValue("@category", (object?)request.Category ?? DBNull.Value);
                    checkCmd.Parameters.AddWithValue("@id", id);
                    var count = (long)(await checkCmd.ExecuteScalarAsync(ct))!;
                    if (count > 0)
                    {
                        var categoryDisplay = request.Category ?? "(no category)";
                        return Conflict(new { Message = $"A tag popularity rule with tag '{request.Tag}' and category '{categoryDisplay}' already exists." });
                    }
                }

                const string sql = @"
UPDATE frl.frl_popularity_tag_rules
SET tag = @tag,
    percentage = @percentage,
    is_active = @is_active,
    category = @category,
    updated_at = now()
WHERE id = @id
RETURNING id, tag, percentage, is_active, created_at, updated_at, category;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@tag", request.Tag.Trim());
                cmd.Parameters.AddWithValue("@percentage", request.Percentage);
                cmd.Parameters.AddWithValue("@is_active", request.IsActive ?? true);
                cmd.Parameters.AddWithValue("@category", (object?)request.Category ?? DBNull.Value);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Tag popularity rule with ID {id} not found." });

                return Ok(MapToDto(reader));
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A tag popularity rule with tag '{request.Tag}' and category '{request.Category ?? "(no category)"}' already exists." });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// DELETE /api/admin/tag-popularity/{id}
        /// Deletes a tag popularity rule by ID.
        /// </summary>
        [HttpDelete("{id:long}")]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<IActionResult> Delete(long id, CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                const string sql = @"DELETE FROM frl.frl_popularity_tag_rules WHERE id = @id;";
                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);

                var rowsAffected = await cmd.ExecuteNonQueryAsync(ct);

                if (rowsAffected == 0)
                    return NotFound(new { Message = $"Tag popularity rule with ID {id} not found." });

                return NoContent();
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// POST /api/admin/tag-popularity/apply
        /// Reads all active rules from frl_popularity_tag_rules updated within the past 6 hours,
        /// then adjusts frl_images.base_weighted_score by the rule's percentage for each image
        /// matched via the rule's category (e.g. join tables, frl_images columns, or frl_movies columns).
        /// </summary>
        /// <summary>
        /// GET /api/admin/tag-popularity/has-unsynced
        /// Returns whether there are any active rules that have not been synced yet.
        /// </summary>
        [HttpGet("has-unsynced")]
        [ProducesResponseType(typeof(HasUnsyncedResult), StatusCodes.Status200OK)]
        public async Task<ActionResult<HasUnsyncedResult>> HasUnsyncedRules(CancellationToken ct)
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
SELECT COUNT(*) FROM frl.frl_popularity_tag_rules
WHERE is_active = true
  AND (last_synced_at IS NULL OR updated_at > last_synced_at);";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                var count = (long)(await cmd.ExecuteScalarAsync(ct))!;
                return Ok(new HasUnsyncedResult { HasUnsynced = count > 0, UnsyncedCount = (int)count });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        [HttpPost("apply")]
        [ProducesResponseType(typeof(ApplyRulesResult), StatusCodes.Status200OK)]
        public async Task<ActionResult<ApplyRulesResult>> ApplyRecentRules(CancellationToken ct)
        {
            var mustClose = false;
            if (_connection.State != ConnectionState.Open)
            {
                await _connection.OpenAsync(ct);
                mustClose = true;
            }

            try
            {
                // Step 1: Read active rules that haven't been synced yet
                const string fetchRulesSql = @"
SELECT id, tag, percentage, category
FROM frl.frl_popularity_tag_rules
WHERE is_active = true
  AND (last_synced_at IS NULL OR updated_at > last_synced_at);";

                var rules = new List<(long Id, string Tag, int Percentage, string? Category)>();

                await using (var cmd = new NpgsqlCommand(fetchRulesSql, _connection))
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                    {
                        rules.Add((
                            reader.GetInt64(0),
                            reader.GetString(1),
                            reader.GetInt32(2),
                            reader.IsDBNull(3) ? null : reader.GetString(3)
                        ));
                    }
                }

                if (rules.Count == 0)
                    return Ok(new ApplyRulesResult { RulesProcessed = 0, TotalImagesUpdated = 0 });

                // Step 2: For each rule, build the correct UPDATE based on category
                var totalUpdated = 0;
                var ruleResults = new List<ApplyRuleDetail>();
                var syncedRuleIds = new List<long>();

                foreach (var rule in rules)
                {
                    var updateSql = BuildUpdateSqlForCategory(rule.Category);
                    if (updateSql == null)
                    {
                        _logger.LogWarning("Rule {RuleId} has unknown category '{Category}', skipping.", rule.Id, rule.Category);
                        ruleResults.Add(new ApplyRuleDetail
                        {
                            RuleId = rule.Id,
                            Tag = rule.Tag,
                            Percentage = rule.Percentage,
                            Category = rule.Category,
                            ImagesUpdated = 0,
                            Skipped = true,
                            SkipReason = $"Unknown category: {rule.Category}"
                        });
                        continue;
                    }

                    await using var updateCmd = new NpgsqlCommand(updateSql, _connection);
                    updateCmd.Parameters.AddWithValue("@percentage", rule.Percentage);
                    updateCmd.Parameters.AddWithValue("@tag", rule.Tag);

                    var rowsAffected = await updateCmd.ExecuteNonQueryAsync(ct);
                    totalUpdated += rowsAffected;
                    syncedRuleIds.Add(rule.Id);

                    ruleResults.Add(new ApplyRuleDetail
                    {
                        RuleId = rule.Id,
                        Tag = rule.Tag,
                        Percentage = rule.Percentage,
                        Category = rule.Category,
                        ImagesUpdated = rowsAffected
                    });
                }

                // Step 3: Mark successfully applied rules as synced
                if (syncedRuleIds.Count > 0)
                {
                    var idList = string.Join(", ", syncedRuleIds);
                    var markSyncedSql = $"UPDATE frl.frl_popularity_tag_rules SET last_synced_at = now() WHERE id IN ({idList});";
                    await using var syncCmd = new NpgsqlCommand(markSyncedSql, _connection);
                    await syncCmd.ExecuteNonQueryAsync(ct);
                }

                return Ok(new ApplyRulesResult
                {
                    RulesProcessed = rules.Count,
                    TotalImagesUpdated = totalUpdated,
                    Details = ruleResults
                });
            }
            finally
            {
                if (mustClose) await _connection.CloseAsync();
            }
        }

        /// <summary>
        /// Returns the UPDATE SQL for a given category, or null if the category is unknown.
        /// Each category maps to a different join table / column to find matching image IDs.
        /// </summary>
        private static string? BuildUpdateSqlForCategory(string? category)
        {
            // Join-table categories: UPDATE frl_images via a join table
            // Image-column categories: UPDATE frl_images matching a column on frl_images directly
            // Movie-column categories: UPDATE frl_images via frl_movies join

            return category switch
            {
                "Time Of Day" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_time_of_day j
WHERE j.imageid = img.idnum AND j.time_of_day = @tag;",

                "Lighting Type" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_lighting_type j
WHERE j.imageid = img.idnum AND j.lighting_type = @tag;",

                "Vfx Backing" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_vfx_backing j
WHERE j.imageid = img.idnum AND j.vfx_backing = @tag;",

                "Color" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_color j
WHERE j.imageid = img.idnum AND j.color = @tag;",

                "Shot Type" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_shot_type j
WHERE j.imageid = img.idnum AND j.shot_type = @tag;",

                "Lighting" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_lighting j
WHERE j.imageid = img.idnum AND j.lighting = @tag;",

                "Lens Size" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_lens_type j
WHERE j.imageid = img.idnum AND j.lens_type = @tag;",

                "Composition" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_composition j
WHERE j.imageid = img.idnum AND j.composition = @tag;",

                "Actors" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
WHERE img.actors ILIKE '%' || @tag || '%';",

                "Int Ext" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
WHERE img.int_ext ILIKE '%' || @tag || '%';",

                "Aspect Ratio" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
WHERE img.aspect_ratio ILIKE '%' || @tag || '%';",

                "Media Type" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.media_type ILIKE '%' || @tag || '%';",

                "Title" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.title ILIKE '%' || @tag || '%';",

                "Director" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.director ILIKE '%' || @tag || '%';",

                "Cinematographer" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.cinematographer ILIKE '%' || @tag || '%';",

                "Production Designer" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.production_designer ILIKE '%' || @tag || '%';",

                "Costume Designer" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.costume_designer ILIKE '%' || @tag || '%';",

                "Mv Artist" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.mv_artist ILIKE '%' || @tag || '%';",

                "Comm Brand" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_movies m
WHERE img.movieid = m.idnum AND m.comm_brand ILIKE '%' || @tag || '%';",

                "Tag" => @"
UPDATE frl.frl_images img
SET weighted_score = img.base_weighted_score + (img.base_weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_tags jit
WHERE jit.imageid = img.idnum AND jit.tag = @tag;",

                _ => null
            };
        }

        #region Helpers

        private static TagPopularityRuleDto MapToDto(NpgsqlDataReader reader)
        {
            return new TagPopularityRuleDto
            {
                Id = reader.GetInt64(0),
                Tag = reader.GetString(1),
                Percentage = reader.GetInt32(2),
                IsActive = reader.GetBoolean(3),
                CreatedAt = reader.GetDateTime(4),
                UpdatedAt = reader.GetDateTime(5),
                Category = reader.IsDBNull(6) ? null : reader.GetString(6)
            };
        }

        #endregion

        #region DTOs

        public sealed class TagPopularityRuleDto
        {
            public long Id { get; set; }
            public string Tag { get; set; } = default!;
            public int Percentage { get; set; }
            public bool IsActive { get; set; }
            public DateTime CreatedAt { get; set; }
            public DateTime UpdatedAt { get; set; }
            public string? Category { get; set; }
        }

        public sealed class CreateTagPopularityRuleRequest
        {
            public string? Tag { get; set; }
            public int Percentage { get; set; }
            public bool? IsActive { get; set; }
            public string? Category { get; set; }
        }

        public sealed class UpdateTagPopularityRuleRequest
        {
            public string? Tag { get; set; }
            public int Percentage { get; set; }
            public bool? IsActive { get; set; }
            public string? Category { get; set; }
        }

        public sealed class HasUnsyncedResult
        {
            public bool HasUnsynced { get; set; }
            public int UnsyncedCount { get; set; }
        }

        public sealed class ApplyRulesResult
        {
            public int RulesProcessed { get; set; }
            public int TotalImagesUpdated { get; set; }
            public List<ApplyRuleDetail> Details { get; set; } = new();
        }

        public sealed class ApplyRuleDetail
        {
            public long RuleId { get; set; }
            public string Tag { get; set; } = default!;
            public int Percentage { get; set; }
            public string? Category { get; set; }
            public int ImagesUpdated { get; set; }
            public bool Skipped { get; set; }
            public string? SkipReason { get; set; }
        }

        #endregion
    }
}
