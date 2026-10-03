using AdminPanelAPI.Models;

namespace AdminPanelAPI.Interfaces
{
    public interface IClipPreviewService
    {
        Task<ClipPreviewResult> GeneratePreviewsForMovieAsync(
            int movieId,
            bool overwrite,
            CancellationToken cancellationToken);

        Task<ClipPreviewBackfillResult> BackfillAsync(
            int movieLimit,
            int afterMovieId,
            bool overwrite,
            CancellationToken cancellationToken);

        Task<ClipPreviewMotionResult> GeneratePreviewBatchAsync(
            int limit,
            int afterImageId,
            bool overwrite,
            bool motionOnly,
            CancellationToken cancellationToken);

        Task<ClipPreviewProgress> GetPreviewProgressAsync(
            int afterImageId,
            int? movieId,
            bool countR2,
            long maxObjectsToCount,
            bool motionOnly,
            CancellationToken cancellationToken);

        Task<ClipPreviewResult> GeneratePreviewsAsync(
            int movieId,
            IReadOnlyCollection<ClipPreviewBoundary> boundaries,
            bool overwrite,
            CancellationToken cancellationToken);
    }
}
