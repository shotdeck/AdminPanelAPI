using AdminPanelAPI.Models;

public interface ICaptionEmbeddingService
{
    Task ProcessBatchAsync(
        long jobId,
        int batchSize,
        CancellationToken cancellationToken);

    Task ProcessAllAsync(
        long jobId,
        int concurrency,
        CancellationToken cancellationToken,
        bool skipCaption = false);

    Task<CaptionEmbeddingResult> ProcessSingleImageAsync(
        int imageId,
        CancellationToken cancellationToken);
}

public class CaptionEmbeddingResult
{
    public int ImageId { get; set; }
    public string Status { get; set; } = "";
    public string? Caption { get; set; }
    public string? KeywordMetadata { get; set; }
    public int? EmbeddingLength { get; set; }
    public int? FusedEmbeddingLength { get; set; }
    public string? Error { get; set; }
}
