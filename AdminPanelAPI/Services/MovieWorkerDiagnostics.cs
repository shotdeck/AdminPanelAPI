using System.Collections.Concurrent;

namespace AdminPanelAPI.Services
{
    /// <summary>
    /// What each movie worker loop last did, so a stuck queue can be told apart
    /// from a worker that never started without reading the host's logs.
    /// </summary>
    public class MovieWorkerDiagnostics
    {
        public class LoopState
        {
            public DateTime StartedAt { get; set; }
            public DateTime? LastPollAt { get; set; }
            public DateTime? LastClaimAt { get; set; }
            public long? CurrentJobId { get; set; }
            public long Polls { get; set; }
            public long Claims { get; set; }
            public DateTime? LastErrorAt { get; set; }
            public string? LastError { get; set; }
        }

        private readonly ConcurrentDictionary<int, LoopState> _loops = new();

        public int ConfiguredParallelism { get; set; }

        public void LoopStarted(int workerNumber)
        {
            _loops[workerNumber] = new LoopState { StartedAt = DateTime.UtcNow };
        }

        public void Polled(int workerNumber)
        {
            if (!_loops.TryGetValue(workerNumber, out var loop))
                return;

            loop.LastPollAt = DateTime.UtcNow;
            loop.Polls++;
            loop.CurrentJobId = null;
        }

        public void Claimed(int workerNumber, long jobId)
        {
            if (!_loops.TryGetValue(workerNumber, out var loop))
                return;

            loop.LastClaimAt = DateTime.UtcNow;
            loop.Claims++;
            loop.CurrentJobId = jobId;
        }

        public void Finished(int workerNumber)
        {
            if (_loops.TryGetValue(workerNumber, out var loop))
                loop.CurrentJobId = null;
        }

        public void Failed(int workerNumber, string error)
        {
            if (!_loops.TryGetValue(workerNumber, out var loop))
                return;

            loop.LastErrorAt = DateTime.UtcNow;
            loop.LastError = error;
            loop.CurrentJobId = null;
        }

        public object Snapshot()
        {
            return new
            {
                configuredParallelism = ConfiguredParallelism,
                loopsStarted = _loops.Count,
                nowUtc = DateTime.UtcNow,
                loops = _loops
                    .OrderBy(entry => entry.Key)
                    .Select(entry => new
                    {
                        worker = entry.Key,
                        startedAt = entry.Value.StartedAt,
                        lastPollAt = entry.Value.LastPollAt,
                        lastClaimAt = entry.Value.LastClaimAt,
                        currentJobId = entry.Value.CurrentJobId,
                        polls = entry.Value.Polls,
                        claims = entry.Value.Claims,
                        lastErrorAt = entry.Value.LastErrorAt,
                        lastError = entry.Value.LastError
                    })
                    .ToList()
            };
        }
    }
}
