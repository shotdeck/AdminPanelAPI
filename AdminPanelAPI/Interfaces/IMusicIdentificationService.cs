public interface IMusicIdentificationService
{
    Task IdentifyMovieAsync(
        long jobId,
        int movieId,
        string? r2Key,
        CancellationToken cancellationToken);
}
