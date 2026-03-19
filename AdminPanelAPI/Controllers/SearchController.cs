using Microsoft.AspNetCore.Mvc;
using ShotDeck.Keywords;

namespace ShotDeckSearch.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public sealed class SearchController : ControllerBase
    {
        private readonly IKeywordCacheService _keywordCache;

        public SearchController(IKeywordCacheService keywordCache)
        {
            _keywordCache = keywordCache;
        }

        [HttpPost("refresh")]
        public async Task<IActionResult> Refresh()
        {
            await _keywordCache.RefreshAsync();
            return Ok("Keyword cache refreshed.");
        }
    }
}
