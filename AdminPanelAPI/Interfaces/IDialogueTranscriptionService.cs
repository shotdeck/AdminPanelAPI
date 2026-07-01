public interface IDialogueTranscriptionService
{
    Task TranscribeMovieAsync(
        long jobId,
        int movieId,
        CancellationToken cancellationToken);
}
