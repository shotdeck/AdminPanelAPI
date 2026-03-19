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
SELECT id, tag, percentage, is_active, created_at, updated_at
FROM frl.frl_popularity_tag_rules
ORDER BY tag;";
                }
                else
                {
                    sql = @"
SELECT id, tag, percentage, is_active, created_at, updated_at
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
SELECT id, tag, percentage, is_active, created_at, updated_at
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
                const string sql = @"
INSERT INTO frl.frl_popularity_tag_rules (tag, percentage, is_active)
VALUES (@tag, @percentage, @is_active)
RETURNING id, tag, percentage, is_active, created_at, updated_at;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@tag", request.Tag.Trim());
                cmd.Parameters.AddWithValue("@percentage", request.Percentage);
                cmd.Parameters.AddWithValue("@is_active", request.IsActive ?? true);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return BadRequest(new { Message = "Failed to create tag popularity rule." });

                var result = MapToDto(reader);
                return CreatedAtAction(nameof(GetById), new { id = result.Id }, result);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A tag popularity rule with tag '{request.Tag}' already exists." });
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
                const string sql = @"
UPDATE frl.frl_popularity_tag_rules
SET tag = @tag,
    percentage = @percentage,
    is_active = @is_active,
    updated_at = now()
WHERE id = @id
RETURNING id, tag, percentage, is_active, created_at, updated_at;";

                await using var cmd = new NpgsqlCommand(sql, _connection);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@tag", request.Tag.Trim());
                cmd.Parameters.AddWithValue("@percentage", request.Percentage);
                cmd.Parameters.AddWithValue("@is_active", request.IsActive ?? true);

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                if (!await reader.ReadAsync(ct))
                    return NotFound(new { Message = $"Tag popularity rule with ID {id} not found." });

                return Ok(MapToDto(reader));
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                return Conflict(new { Message = $"A tag popularity rule with tag '{request.Tag}' already exists." });
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
        /// that has the matching tag in frl_join_images_tags.
        /// </summary>
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
                // Step 1: Read active rules updated within the past 6 hours
                const string fetchRulesSql = @"
SELECT id, tag, percentage
FROM frl.frl_popularity_tag_rules
WHERE is_active = true
  AND updated_at >= now() - interval '6 hours';";

                var rules = new List<(long Id, string Tag, int Percentage)>();

                await using (var cmd = new NpgsqlCommand(fetchRulesSql, _connection))
                await using (var reader = await cmd.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                    {
                        rules.Add((reader.GetInt64(0), reader.GetString(1), reader.GetInt32(2)));
                    }
                }

                if (rules.Count == 0)
                    return Ok(new ApplyRulesResult { RulesProcessed = 0, TotalImagesUpdated = 0 });

                // Step 2: For each rule, update frl_images.base_weighted_score
                const string updateSql = @"
UPDATE frl.frl_images img
SET base_weighted_score = img.weighted_score + (img.weighted_score * @percentage / 100.0)
FROM frl.frl_join_images_tags jit
WHERE jit.imageid = img.idnum
  AND jit.tag = @tag;";

                var totalUpdated = 0;
                var ruleResults = new List<ApplyRuleDetail>();

                foreach (var rule in rules)
                {
                    await using var updateCmd = new NpgsqlCommand(updateSql, _connection);
                    updateCmd.Parameters.AddWithValue("@percentage", rule.Percentage);
                    updateCmd.Parameters.AddWithValue("@tag", rule.Tag);

                    var rowsAffected = await updateCmd.ExecuteNonQueryAsync(ct);
                    totalUpdated += rowsAffected;

                    ruleResults.Add(new ApplyRuleDetail
                    {
                        RuleId = rule.Id,
                        Tag = rule.Tag,
                        Percentage = rule.Percentage,
                        ImagesUpdated = rowsAffected
                    });
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
                UpdatedAt = reader.GetDateTime(5)
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
        }

        public sealed class CreateTagPopularityRuleRequest
        {
            public string? Tag { get; set; }
            public int Percentage { get; set; }
            public bool? IsActive { get; set; }
        }

        public sealed class UpdateTagPopularityRuleRequest
        {
            public string? Tag { get; set; }
            public int Percentage { get; set; }
            public bool? IsActive { get; set; }
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
            public int ImagesUpdated { get; set; }
        }

        #endregion
    }
}
