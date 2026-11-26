using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal delegate Task<ConsumingResult> KafkaMessageHandler(ConsumeResult<Ignore, byte[]> consumeResult, CancellationToken stopToken);

internal sealed class KafkaConsumeLoop
    (KafkaReceiverOptions options, IKafkaConsumerFactory consumerFactory, KafkaMessageHandler handler)
{
    private static readonly ILogger Logger = LogManager.GetLogger<KafkaConsumeLoop>();

    private static readonly TimeSpan PollTimeout = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan PausedPollTimeout = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan MinConsumeErrorBackoff = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan MaxConsumeErrorBackoff = TimeSpan.FromSeconds(5);

    private readonly bool autoCommit = options.EnableAutoCommit == true;
    private readonly PartitionOffsetTracker tracker = new();
    private readonly DeferredAcknowledgements deferred = new();

    private IConsumer<Ignore, byte[]> consumer;

    private int handledMessagesSinceLastCommit;
    private DateTime lastCommitTime = DateTime.UtcNow;

    private TimeSpan errorBackoff = TimeSpan.Zero;

    private bool paused;

    public void Prepare()
    {
        try
        {
            this.consumer = consumerFactory.Create(options, CommitRevokedPartitions);
            this.consumer.Subscribe(options.Label);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to topic {Label} at [{ConnectionString}]", options.Label, options.ConnectionString);
            CloseConsumer();
            throw;
        }

        Logger.LogInformation("Subscribed to topic {Label}. GroupId: {GroupId}, PartitionAssignmentStrategy: {Strategy}", options.Label, options.GroupId, options.PartitionAssignmentStrategy);
    }

    public async Task Run(CancellationToken stopToken)
    {
        try
        {
            await Consume(stopToken);

            // Решения, отложенные обработчиком, принимаются и на остановке, чтобы попасть в финальный коммит.
            await WaitForDeferred();
            Checkpoint(force: true);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Consumer at [{ConnectionString}] on topic {Label} has failed", options.ConnectionString, options.Label);
        }
        finally
        {
            CloseConsumer();
        }

        Logger.LogInformation("Stopped consuming at [{ConnectionString}] on topic {Label}", options.ConnectionString, options.Label);
    }

    private async Task Consume(CancellationToken stopToken)
    {
        while (!stopToken.IsCancellationRequested)
        {
            ApplyBackpressure();

            ConsumeResult<Ignore, byte[]> consumeResult;

            try
            {
                consumeResult = this.consumer.Consume(this.paused ? PausedPollTimeout : PollTimeout);
            }
            catch (ConsumeException ex)
            {
                Logger.LogError(ex, "Consume at [{ConnectionString}] error: {Reason}", options.ConnectionString, ex.Error.Reason);

                await WaitOnError(stopToken);
                continue;
            }

            this.errorBackoff = TimeSpan.Zero;

            if (consumeResult == null)
            {
                Checkpoint(force: false);
                continue;
            }

            // Конец партиции приходит, только если явно включён EnablePartitionEof (по умолчанию выключен).
            if (consumeResult.IsPartitionEOF)
            {
                Checkpoint(force: true);
                continue;
            }

            await Handle(consumeResult, stopToken);
            Checkpoint(force: false);
        }
    }

    private async Task Handle(ConsumeResult<Ignore, byte[]> consumeResult, CancellationToken stopToken)
    {
        this.tracker.Observe(consumeResult);

        try
        {
            var handlingResult = await handler(consumeResult, stopToken);

            if (handlingResult.IsCompleted)
            {
                if (handlingResult.IsAcknowledged!.Value)
                {
                    this.tracker.Acknowledge(consumeResult);
                }

                this.handledMessagesSinceLastCommit++;
                return;
            }

            // Обработчик вернул управление, не подтвердив сообщение: подтверждение придёт позже
            // (батчевая обработка), воркер его не ждёт и читает дальше.
            this.deferred.Track(consumeResult, handlingResult);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Unexpected error when processing a message from topic {Label} at offset {Offset}", consumeResult.Topic, consumeResult.Offset);
        }
    }

    private void Checkpoint(bool force)
    {
        while (this.deferred.TryDequeueAcknowledged(out var acknowledged))
        {
            this.tracker.Acknowledge(acknowledged);
            this.handledMessagesSinceLastCommit++;
        }

        var shouldCommit = this.handledMessagesSinceLastCommit >= options.CommitOffset
                  || (this.handledMessagesSinceLastCommit > 0 && DateTime.UtcNow - this.lastCommitTime >= options.CommitTimeout);

        if (!force && !shouldCommit)
        {
            return;
        }

        var positions = this.tracker.GetCommitPositions();

        // Позиции отмечаются зафиксированными только после успешного коммита, иначе
        // сорвавшийся коммит больше не повторился бы.
        if (TryCommit(positions))
        {
            this.tracker.Committed(positions);
        }

        // счётчик сбрасывается и после неуспешного коммита, чтобы не пытаться коммитить слишком часто при постоянных ошибках.
        this.handledMessagesSinceLastCommit = 0;
        this.lastCommitTime = DateTime.UtcNow;
    }

    /// <summary>
    ///   Приостанавливает чтение, пока отложенных подтверждений не меньше
    ///   <see cref="KafkaReceiverOptions.MaxPendingAcknowledgements"/>: иначе батчевый обработчик,
    ///   не успевающий за потоком, копил бы в памяти весь топик. Poll продолжается, поэтому воркер
    ///   не выпадает из группы по <c>max.poll.interval.ms</c> и видит ребалансы.
    /// </summary>
    /// <remarks>
    ///   Пауза повторяется на каждом витке: партиции, назначенные ребалансом, приходят неприостановленными.
    /// </remarks>
    private void ApplyBackpressure()
    {
        var pending = this.deferred.Pending;
        var limit = options.MaxPendingAcknowledgements;

        try
        {
            if (pending >= limit)
            {
                if (!this.paused)
                {
                    Logger.LogInformation(
                        "Pausing topic {Label}: {PendingCount} message(s) are awaiting acknowledgement, the limit is {Limit}",
                        options.Label,
                        pending,
                        limit);
                }

                this.consumer.Pause(this.consumer.Assignment);
                this.paused = true;

                return;
            }

            if (this.paused)
            {
                this.consumer.Resume(this.consumer.Assignment);
                this.paused = false;

                Logger.LogInformation(
                    "Resuming topic {Label}: {PendingCount} message(s) are awaiting acknowledgement, the limit is {Limit}",
                    options.Label,
                    pending,
                    limit);
            }
        }
        catch (KafkaException ex)
        {
            // Посреди ребаланса назначение может разойтись с брокером: повторим на следующем витке.
            Logger.LogWarning(ex, "Failed to pause or resume topic {Label}: {Reason}", options.Label, ex.Error.Reason);
        }
    }

    private async Task WaitForDeferred()
    {
        var pending = this.deferred.Pending;

        if (pending == 0)
        {
            return;
        }

        Logger.LogInformation("Waiting for {PendingCount} deferred acknowledgement(s) of topic {Label}", pending, options.Label);

        await this.deferred.WhenDrained();
    }

    private void CommitRevokedPartitions(IConsumer<Ignore, byte[]> _, List<TopicPartitionOffset> revokedPartitions)
    {
        var positions = this.tracker.DrainCommitPositions(revokedPartitions.Select(p => p.TopicPartition));

        if (positions.Count > 0)
        {
            Logger.LogInformation("Committing {PositionsCount} position(s) for revoked partitions of topic {Label}: {Positions}", positions.Count, options.Label, string.Join(", ", positions));

            TryCommit(positions);
        }
    }

    /// <summary>
    ///   Возвращает <c>true</c>, если позиции зафиксированы брокером (или, при автокоммите,
    ///   переданы клиенту для фиксации).
    /// </summary>
    private bool TryCommit(IReadOnlyCollection<TopicPartitionOffset> positions)
    {
        if (positions.Count == 0)
        {
            return false;
        }

        try
        {
            if (this.autoCommit)
            {
                foreach (var position in positions)
                {
                    this.consumer.StoreOffset(position);
                }

                Logger.LogDebug("Stored {Positions} for auto-commit. GroupId: {GroupId}", string.Join(", ", positions), options.GroupId);

                return true;
            }

            this.consumer.Commit(positions);

            Logger.LogDebug("Committed {Positions}. GroupId: {GroupId}", string.Join(", ", positions), options.GroupId);

            return true;
        }
        catch (KafkaException ex)
        {
            Logger.LogError(ex, "Commit error at [{ConnectionString}]: {Reason}. GroupId: {GroupId}, positions: {Positions}", options.ConnectionString, ex.Error.Reason, options.GroupId, string.Join(", ", positions));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Unexpected error during commit at [{ConnectionString}]. GroupId: {GroupId}, positions: {Positions}", options.ConnectionString, options.GroupId, string.Join(", ", positions));
        }

        return false;
    }

    /// <summary>
    ///   Пауза после ошибки чтения, с каждой ошибкой подряд вдвое длиннее. Остановка шины
    ///   прерывает её сразу.
    /// </summary>
    private async Task WaitOnError(CancellationToken stopToken)
    {
        this.errorBackoff = this.errorBackoff == TimeSpan.Zero
            ? MinConsumeErrorBackoff
            : TimeSpan.FromTicks(Math.Min(this.errorBackoff.Ticks * 2, MaxConsumeErrorBackoff.Ticks));

        try
        {
            await Task.Delay(this.errorBackoff, stopToken);
        }
        catch (OperationCanceledException)
        {
            // Остановка во время паузы: цикл увидит отменённый токен и завершится.
        }
    }

    private void CloseConsumer()
    {
        if (this.consumer == null)
        {
            return;
        }

        try
        {
            // Без Close брокер ждёт session timeout, прежде чем перераспределить партиции.
            this.consumer.Close();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Error while closing consumer of topic {Label}", options.Label);
        }
        finally
        {
            try
            {
                this.consumer.Dispose();
            }
            catch (Exception e)
            {
                // ignore
            }
        }
    }
}
