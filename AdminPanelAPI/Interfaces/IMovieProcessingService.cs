using AdminPanelAPI.Models;

public interface IMovieProcessingService
{
    Task ProcessMovieAsync(
        long jobId,
        int movieId,
        double threshold,
        bool overwrite,
        bool missingOnly,
        CancellationToken cancellationToken);

    Task<MovieMissingClipReport> GetMissingClipReportAsync(
        int movieId,
        CancellationToken cancellationToken);
}
