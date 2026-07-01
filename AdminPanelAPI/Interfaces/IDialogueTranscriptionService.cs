public interface IDialogueTranscriptionService
{
    Task TranscribeMovieAsync(
        long jobId,
        int movieId,
        string? r2Key,
        CancellationToken cancellationToken);
}
