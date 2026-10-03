using AdminPanelAPI.Models;

namespace AdminPanelAPI.Interfaces
{
    /// <summary>
    /// Runs the clip-preview backfill unattended: one start call walks every
    /// eligible clip in batches until none are left, so no caller has to feed the
    /// cursor back in. Eligibility is either motion-tagged clips or, with
    /// motionOnly off, every clip with a usable scene boundary.
    /// </summary>
    public interface IClipPreviewMotionRunner
    {
        /// <summary>
        /// Starts a run. Returns false with the live status when one is already going.
        /// </summary>
        (bool Started, ClipPreviewRunStatus Status) Start(
            int afterImageId,
            int batchSize,
            bool overwrite,
            bool motionOnly);

        /// <summary>Asks the run to stop after the batch it is on.</summary>
        ClipPreviewRunStatus Stop();

        ClipPreviewRunStatus GetStatus();
    }
}
