public interface ICaptionEmbeddingService
{
    Task ProcessBatchAsync(
        long jobId,
        int batchSize,
        CancellationToken cancellationToken);
}
