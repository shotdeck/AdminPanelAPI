public interface IMovieProcessingService
{
    Task ProcessMovieAsync(
        long jobId,
        int movieId,
        double threshold,
        bool overwrite,
        CancellationToken cancellationToken);
}