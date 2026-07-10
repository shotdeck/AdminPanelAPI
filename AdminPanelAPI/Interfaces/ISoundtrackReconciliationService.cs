using AdminPanelAPI.Models;

public interface ISoundtrackReconciliationService
{
    /// <summary>
    /// Reconcile a movie's identified tracks against its known soundtrack
    /// (Wikipedia), tag each occurrence with a confidence level, and return a
    /// report. Non-destructive: nothing is deleted.
    /// </summary>
    Task<ReconcileResult> ReconcileAsync(int movieId, CancellationToken cancellationToken);
}
