using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Подтверждения, которые обработчик вынес за пределы HandleAsync (так работают батчевые
///   обработчики). Решения приходят с чужих потоков, а трекер оффсетов принадлежит воркеру,
///   поэтому подтверждённые сообщения складываются в очередь и разбираются воркером.
///   Хранится только счётчик ожидающих, а не их задачи, чтобы не держать их в памяти.
/// </summary>
internal sealed class DeferredAcknowledgements
{
    private static readonly ILogger Logger = LogManager.GetLogger<DeferredAcknowledgements>();

    private readonly ConcurrentQueue<ConsumeResult<Ignore, byte[]>> acknowledged = new();

    private int pending;

    private TaskCompletionSource drained;

    public int Pending => Volatile.Read(ref this.pending);

    public void Track(ConsumeResult<Ignore, byte[]> consumeResult, ConsumingResult consumingResult)
    {
        Interlocked.Increment(ref this.pending);
        _ = Await(consumeResult, consumingResult);
    }

    public bool TryDequeueAcknowledged(out ConsumeResult<Ignore, byte[]> consumeResult)
    {
        return this.acknowledged.TryDequeue(out consumeResult);
    }

    public Task WhenDrained()
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Exchange — полный барьер: последнее решение либо увидит сигнал, либо уже обнулило
        // счётчик, и тогда это видно здесь.
        Interlocked.Exchange(ref this.drained, signal);

        if (Pending == 0)
        {
            signal.TrySetResult();
        }

        return signal.Task;
    }

    private async Task Await(ConsumeResult<Ignore, byte[]> consumeResult, ConsumingResult consumingResult)
    {
        try
        {
            if (await consumingResult.AcknowledgeTask)
            {
                this.acknowledged.Enqueue(consumeResult);
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(
                ex,
                "Deferred acknowledgement of a message from topic {Label} at offset {Offset} has failed, its offset is held",
                consumeResult.Topic,
                consumeResult.Offset);
        }
        finally
        {
            if (Interlocked.Decrement(ref this.pending) == 0)
            {
                Volatile.Read(ref this.drained)?.TrySetResult();
            }
        }
    }
}
