using System.Threading.Channels;

public interface IDialogueJobQueue
{
    ValueTask QueueJobAsync(long jobId, CancellationToken cancellationToken = default);
    ValueTask<long> DequeueAsync(CancellationToken cancellationToken);
}

public class DialogueJobQueue : IDialogueJobQueue
{
    private readonly Channel<long> _queue = Channel.CreateUnbounded<long>();

    public async ValueTask QueueJobAsync(long jobId, CancellationToken cancellationToken = default)
    {
        await _queue.Writer.WriteAsync(jobId, cancellationToken);
    }

    public async ValueTask<long> DequeueAsync(CancellationToken cancellationToken)
    {
        return await _queue.Reader.ReadAsync(cancellationToken);
    }
}
