using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Receiving;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.Kafka.Internal;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

[TestFixture]
[Category("Unit")]
public class KafkaReceivingSourceTests
{
    private const string Topic = "test-topic";

    private FakeKafkaConsumer consumer;
    private FakeKafkaConsumerFactory factory;
    private TestKafkaConsumerOf<string> messageConsumer;
    private int activeHandlers;

    [SetUp]
    public void SetUp()
    {
        this.consumer = new FakeKafkaConsumer();
        this.factory = new FakeKafkaConsumerFactory();
        this.factory.Prepare(this.consumer);
        this.messageConsumer = new TestKafkaConsumerOf<string>();
        this.activeHandlers = 0;
    }

    private KafkaReceivingSource CreateSource(
        bool? enableAutoCommit = false,
        int commitOffset = 1000,
        TimeSpan? commitTimeout = null,
        IKafkaConsumerFactory consumerFactory = null,
        IFailedDeliveryStrategy failedDeliveryStrategy = null,
        int maxPendingAcknowledgements = 10_000,
        bool acceptRequired = false)
    {
        var options = new KafkaReceiverOptions
        {
            Label = MessageLabel.From(Topic),
            GroupId = "test-group",
            ConnectionString = "fake:9092",
            EnableAutoCommit = enableAutoCommit,
            CommitOffset = commitOffset,
            CommitTimeout = commitTimeout ?? TimeSpan.FromSeconds(5),
            ParallelismLevel = 1,
            PayloadConverter = new PassThroughPayloadConverter(),
            FailedDeliveryStrategy = failedDeliveryStrategy,
            MaxPendingAcknowledgements = maxPendingAcknowledgements,
            AcceptRequired = acceptRequired
        };

        var source = new KafkaReceivingSource(null, options.ConnectionString, consumerFactory ?? this.factory, options);
        source.OnStartConsuming += _ => Interlocked.Increment(ref this.activeHandlers);
        source.OnFinishConsuming += _ => Interlocked.Decrement(ref this.activeHandlers);
        source.RegisterConsumer(this.messageConsumer);

        return source;
    }

    private async Task<KafkaReceivingSource> StartSource(
        bool? enableAutoCommit = false,
        int commitOffset = 1000,
        TimeSpan? commitTimeout = null,
        IFailedDeliveryStrategy failedDeliveryStrategy = null,
        int maxPendingAcknowledgements = 10_000,
        bool acceptRequired = false)
    {
        var source = CreateSource(
            enableAutoCommit,
            commitOffset,
            commitTimeout,
            failedDeliveryStrategy: failedDeliveryStrategy,
            maxPendingAcknowledgements: maxPendingAcknowledgements,
            acceptRequired: acceptRequired);
        await source.Prepare(CancellationToken.None);
        await source.StartConsuming(CancellationToken.None);

        return source;
    }

    /// <summary>
    ///   Стратегия, оставляющая упавшее сообщение на повторное чтение: оффсет не продвигается.
    /// </summary>
    private static IFailedDeliveryStrategy HoldingOffsetStrategy()
    {
        return new LambdaFailedDeliveryStrategy(context => context.RejectAsync(requeue: true));
    }

    private int ActiveHandlers => Volatile.Read(ref this.activeHandlers);

    /// <summary>
    ///   Ведёт себя как батчевый обработчик: возвращает управление сразу, а сообщения
    ///   подтверждает позже — после <see cref="Release"/>.
    /// </summary>
    private sealed class DeferredAccepts
    {
        private readonly ConcurrentQueue<IConsumingContext<string>> pending = new();

        private volatile bool released;

        public Task Handle(IConsumingContext<string> context)
        {
            if (this.released)
            {
                return context.AcceptAsync();
            }

            this.pending.Enqueue(context);

            if (this.released)
            {
                AcceptPending();
            }

            return Task.CompletedTask;
        }

        public void Release()
        {
            this.released = true;
            AcceptPending();
        }

        private void AcceptPending()
        {
            while (this.pending.TryDequeue(out var context))
            {
                context.AcceptAsync();
            }
        }
    }

    private bool HasCommit(int partition, long position)
    {
        return this.consumer.Commits.Any(c => c.Partition.Value == partition && (long)c.Offset == position);
    }

    [Test]
    public async Task Consumes_and_dispatches_message_to_registered_consumer()
    {
        var source = await StartSource();

        this.consumer.Enqueue(Topic, 1, "hello");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        await source.StopConsuming(CancellationToken.None);

        var handled = this.messageConsumer.Handled.Should().ContainSingle().Subject;
        handled.Payload.Should().Be("hello");
        handled.Label.Name.Should().Be(Topic);
        this.consumer.SubscribedTopics.Should().Contain(Topic);
    }

    [Test]
    public async Task Start_fails_when_the_consumer_cannot_be_created()
    {
        var source = CreateSource(consumerFactory: new ThrowingKafkaConsumerFactory(
            new InvalidOperationException("No such configuration property: \"max.poll.records\"")));
        await source.Prepare(CancellationToken.None);

        // Ошибка конфигурации консьюмера должна дойти до того, кто запускает шину: иначе
        // источник молча остаётся мёртвым, а шина считается запущенной.
        await source.Invoking(l => l.StartConsuming(CancellationToken.None))
            .Should().ThrowAsync<InvalidOperationException>();

        source.IsHealthy.Should().BeFalse();
    }

    [Test]
    public async Task Persistent_consume_error_is_retried_with_a_backoff()
    {
        this.consumer.PermanentConsumeException = new ConsumeException(
            new ConsumeResult<byte[], byte[]>(),
            new Error(ErrorCode.Local_AllBrokersDown, "all brokers down"));

        var source = await StartSource();

        await Task.Delay(TimeSpan.FromMilliseconds(600));
        await source.StopConsuming(CancellationToken.None);

        // Устойчивая ошибка возвращается мгновенно: без паузы цикл крутился бы тысячи раз
        // за это время, сжигая ядро и заливая журнал.
        this.consumer.ConsumeCount.Should().BeLessThan(20);
    }

    [Test]
    public async Task Failed_commit_is_retried_instead_of_being_dropped()
    {
        var source = await StartSource(enableAutoCommit: false, commitOffset: 1);

        this.consumer.FailNextCommit(new KafkaException(new Error(ErrorCode.Local_State, "commit failed")));

        this.consumer.Enqueue(Topic, 1, "hello");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        await source.StopConsuming(CancellationToken.None);

        // Чекпоинт сорвался, но позиция не потеряна: её предлагает финальный коммит.
        HasCommit(0, 2).Should().BeTrue("a failed commit must be offered again");
    }

    [Test]
    public async Task Commits_manually_every_commit_offset_messages()
    {
        var source = await StartSource(enableAutoCommit: false, commitOffset: 2);

        for (var offset = 1; offset <= 5; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.messageConsumer.Handled.Count == 5);
        await Wait.Until(() => this.consumer.Commits.Count >= 2, because: "checkpoint commits expected");

        // Коммитится позиция (последний обработанный + 1): чекпоинты после оффсетов 2 и 4.
        HasCommit(0, 3).Should().BeTrue();
        HasCommit(0, 5).Should().BeTrue();

        await source.StopConsuming(CancellationToken.None);
    }

    [Test]
    public async Task Commits_position_of_last_handled_message_on_graceful_stop()
    {
        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000);

        for (var offset = 1; offset <= 3; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);
        this.consumer.Commits.Should().BeEmpty("no checkpoint is expected before stop");

        await source.StopConsuming(CancellationToken.None);

        HasCommit(0, 4).Should()
            .BeTrue("position after the last handled message must be committed on shutdown");
    }

    [Test]
    public async Task Graceful_stop_commits_positions_for_every_partition()
    {
        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000);

        this.consumer.Enqueue(Topic, 1, "p0-m1", partition: 0);
        this.consumer.Enqueue(Topic, 2, "p0-m2", partition: 0);
        this.consumer.Enqueue(Topic, 5, "p1-m1", partition: 1);

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);
        await source.StopConsuming(CancellationToken.None);

        HasCommit(0, 3).Should().BeTrue("partition 0 must be committed on shutdown");
        HasCommit(1, 6).Should().BeTrue("partition 1 must be committed on shutdown too");
    }

    [Test]
    public async Task Commits_pending_tail_by_commit_timeout_on_idle_partition()
    {
        // Хвост тихой партиции не должен висеть незакоммиченным до кратного оффсета.
        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000, commitTimeout: TimeSpan.FromMilliseconds(200));

        for (var offset = 1; offset <= 3; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);

        // Без остановки и без чекпоинта по количеству: коммит должен прийти по таймеру.
        await Wait.Until(() => HasCommit(0, 4), because: "idle tail must be committed by CommitTimeout");

        HasCommit(0, 4).Should().BeTrue();

        await source.StopConsuming(CancellationToken.None);
    }

    [Test]
    public async Task Commits_deferred_tail_by_commit_timeout_on_idle_partition()
    {
        // Накопительный обработчик подтверждает пачку позже, когда партиция уже затихла:
        // подтверждения должны попасть в коммит по таймеру, а не ждать следующего сообщения.
        var accepts = new DeferredAccepts();
        this.messageConsumer.Callback = accepts.Handle;

        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000, commitTimeout: TimeSpan.FromMilliseconds(200), acceptRequired: true);

        for (var offset = 1; offset <= 3; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);
        await Task.Delay(300);

        accepts.Release();

        await Wait.Until(() => HasCommit(0, 4), because: "deferred acknowledgements of an idle tail must be committed by CommitTimeout");

        HasCommit(0, 4).Should().BeTrue();

        await source.StopConsuming(CancellationToken.None);
    }

    [Test]
    public async Task Checkpoint_triggers_by_handled_count_not_offset_multiples()
    {
        // Оффсеты с дырками (компактированный/транзакционный топик): ни один не кратен CommitOffset.
        var source = await StartSource(enableAutoCommit: false, commitOffset: 3);

        this.consumer.Enqueue(Topic, 7, "m1");
        this.consumer.Enqueue(Topic, 11, "m2");
        this.consumer.Enqueue(Topic, 13, "m3");

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);
        await Wait.Until(() => HasCommit(0, 14), because: "checkpoint must fire after 3 handled messages regardless of offset values");

        HasCommit(0, 14).Should()
            .BeTrue("the checkpoint must fire after 3 handled messages regardless of offset values");

        await source.StopConsuming(CancellationToken.None);
    }

    [Test]
    public async Task Revoke_commits_tracked_positions_of_revoked_partitions()
    {
        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000);

        this.consumer.Enqueue(Topic, 1, "p0-m1", partition: 0);
        this.consumer.Enqueue(Topic, 7, "p1-m1", partition: 1);
        await Wait.Until(() => this.messageConsumer.Handled.Count == 2);

        // Ребаланс отзывает только партицию 0 — её обработанный хвост коммитится сразу.
        this.consumer.EnqueueRevoke(new TopicPartitionOffset(Topic, new Partition(0), new Offset(1)));

        await Wait.Until(() => HasCommit(0, 2), because: "revoked partition must be committed during rebalance");
        HasCommit(0, 2).Should().BeTrue("the revoked partition must be committed during the rebalance");
        HasCommit(1, 8).Should().BeFalse("partition 1 is not revoked and must not be committed yet");

        await source.StopConsuming(CancellationToken.None);

        HasCommit(1, 8).Should().BeTrue("partition 1 is committed on shutdown");
        this.consumer.Commits.Count(c => c.Partition.Value == 0)
            .Should()
            .Be(1, "the revoked partition is drained from the tracker and must not be re-committed on shutdown");
    }

    [Test]
    public async Task Revoke_stores_tracked_positions_when_autocommit_is_enabled()
    {
        var source = await StartSource(enableAutoCommit: true);

        this.consumer.Enqueue(Topic, 1, "m1");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        this.consumer.EnqueueRevoke(new TopicPartitionOffset(Topic, new Partition(0), new Offset(1)));
        await Wait.Until(() => this.consumer.StoredOffsets.Any(o => (long)o.Offset == 2), because: "the revoked partition's position must be handed to auto-commit");

        await source.StopConsuming(CancellationToken.None);

        this.consumer.Commits.Should().BeEmpty();
    }

    [Test]
    public async Task Autocommit_stores_only_positions_of_handled_messages()
    {
        // При автокоммите librdkafka коммитит только то, что сохранено: упавшее сообщение
        // должно удерживать позицию, как и при ручном коммите.
        this.messageConsumer.Callback = context =>
            context.Message.Payload == "m2" ? throw new InvalidOperationException("boom") : Task.CompletedTask;

        var source = await StartSource(enableAutoCommit: true, commitTimeout: TimeSpan.FromMilliseconds(100), failedDeliveryStrategy: HoldingOffsetStrategy());

        for (var offset = 1; offset <= 3; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);
        await source.StopConsuming(CancellationToken.None);

        this.consumer.StoredOffsets.Should().NotBeEmpty();
        this.consumer.StoredOffsets.Max(o => (long)o.Offset).Should().Be(2, "the failed message at offset 2 holds the position");
        this.consumer.Commits.Should().BeEmpty();
    }

    [Test]
    public async Task Failed_last_message_is_not_committed_on_stop()
    {
        this.messageConsumer.Callback = _ => throw new InvalidOperationException("boom");

        var source = await StartSource(failedDeliveryStrategy: HoldingOffsetStrategy());

        this.consumer.Enqueue(Topic, 1, "poison");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        await source.StopConsuming(CancellationToken.None);

        this.consumer.Commits.Should()
            .BeEmpty("the position of a failed message must not be committed: it will be retried after restart");
    }

    [Test]
    public async Task Failed_message_holds_the_commit_position_even_if_later_ones_succeed()
    {
        this.messageConsumer.Callback = context =>
            context.Message.Payload == "poison" ? throw new InvalidOperationException("boom") : Task.CompletedTask;

        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000, failedDeliveryStrategy: HoldingOffsetStrategy());

        this.consumer.Enqueue(Topic, 1, "poison");
        this.consumer.Enqueue(Topic, 2, "ok");

        await Wait.Until(() => this.messageConsumer.Handled.Count == 2);
        await source.StopConsuming(CancellationToken.None);

        // Коммитится наименьший неподтверждённый оффсет, поэтому успешное сообщение
        // не сдвигает позицию за упавшее: то не будет потеряно.
        HasCommit(0, 3).Should().BeFalse("the failed offset still holds the position");
        this.consumer.Commits.Should().BeEmpty("the position has not moved from where reading started");
    }

    [Test]
    public async Task Position_advances_past_a_gap_once_everything_received_is_handled()
    {
        var source = await StartSource(enableAutoCommit: false, commitOffset: 1000);

        // Пропуски в оффсетах нормальны: компактированные и транзакционные топики.
        this.consumer.Enqueue(Topic, 7, "m1");
        this.consumer.Enqueue(Topic, 11, "m2");
        this.consumer.Enqueue(Topic, 13, "m3");

        await Wait.Until(() => this.messageConsumer.Handled.Count == 3);
        await source.StopConsuming(CancellationToken.None);

        HasCommit(0, 14).Should().BeTrue("gaps must not block the position");
    }

    [Test]
    public async Task Does_not_commit_manually_when_autocommit_is_enabled()
    {
        var source = await StartSource(enableAutoCommit: true);

        this.consumer.Enqueue(Topic, 1, "m1");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        await source.StopConsuming(CancellationToken.None);

        this.consumer.Commits.Should().BeEmpty();
        this.consumer.StoredOffsets.Should().Contain(o => (long)o.Offset == 2, "the final position is handed to auto-commit on stop");
    }

    [Test]
    public async Task Closes_and_disposes_consumer_on_stop()
    {
        var source = await StartSource();
        await source.StopConsuming(CancellationToken.None);

        this.consumer.IsClosed.Should().BeTrue("the consumer must leave the group gracefully via Close()");
        this.consumer.IsDisposed.Should().BeTrue();
        source.IsHealthy.Should().BeFalse();
    }

    [Test]
    public async Task Graceful_stop_waits_for_inflight_handler()
    {
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        this.messageConsumer.Callback = async _ =>
        {
            handlerEntered.TrySetResult();
            await handlerGate.Task;
        };

        var source = await StartSource();

        this.consumer.Enqueue(Topic, 1, "slow");
        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var stopTask = source.StopConsuming(CancellationToken.None);
        await Task.Delay(200);

        stopTask.IsCompleted.Should().BeFalse("stop must wait for the in-flight handler");
        ActiveHandlers.Should().Be(1);

        handlerGate.TrySetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));

        ActiveHandlers.Should().Be(0);
        HasCommit(0, 2).Should().BeTrue();
        this.consumer.IsClosed.Should().BeTrue();
    }

    [Test]
    public async Task Pauses_reading_while_deferred_decisions_reach_the_limit()
    {
        var accepts = new DeferredAccepts();
        this.messageConsumer.Callback = accepts.Handle;

        var source = await StartSource(maxPendingAcknowledgements: 2, acceptRequired: true);

        for (var offset = 1; offset <= 5; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.consumer.IsPaused, because: "the worker must pause once two decisions are pending");
        await Task.Delay(300);

        this.messageConsumer.Handled.Should().HaveCount(2, "nothing must be read while the limit is reached");

        accepts.Release();

        await Wait.Until(() => this.messageConsumer.Handled.Count == 5, because: "reading must resume once decisions are made");
        await source.StopConsuming(CancellationToken.None);

        this.consumer.ResumeCount.Should().BeGreaterThan(0);
        HasCommit(0, 6).Should().BeTrue();
    }

    [Test]
    public async Task Does_not_pause_while_deferred_decisions_are_below_the_limit()
    {
        var accepts = new DeferredAccepts();
        this.messageConsumer.Callback = accepts.Handle;

        var source = await StartSource(maxPendingAcknowledgements: 10, acceptRequired: true);

        for (var offset = 1; offset <= 5; offset++)
        {
            this.consumer.Enqueue(Topic, offset, $"m{offset}");
        }

        await Wait.Until(() => this.messageConsumer.Handled.Count == 5);

        this.consumer.PauseCount.Should().Be(0);

        accepts.Release();
        await source.StopConsuming(CancellationToken.None);
    }

    [Test]
    public async Task Handles_messages_one_at_a_time_while_the_handler_is_running()
    {
        // Асинхронный обработчик не отложенное подтверждение: следующее сообщение не читается,
        // пока предыдущее обрабатывается, иначе нарушился бы порядок внутри партиции.
        var handlerGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.messageConsumer.Callback = async _ => await handlerGate.Task;

        var source = await StartSource();

        this.consumer.Enqueue(Topic, 1, "m1");
        this.consumer.Enqueue(Topic, 2, "m2");

        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);
        await Task.Delay(300);

        this.messageConsumer.Handled.Should().HaveCount(1, "the second message must wait for the first handler");

        handlerGate.TrySetResult();

        await Wait.Until(() => this.messageConsumer.Handled.Count == 2);
        await source.StopConsuming(CancellationToken.None);

        HasCommit(0, 3).Should().BeTrue();
    }

    [Test]
    public async Task Graceful_stop_can_be_abandoned_by_cancellation_token()
    {
        var handlerGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        this.messageConsumer.Callback = async _ => await handlerGate.Task;

        var source = await StartSource();

        this.consumer.Enqueue(Topic, 1, "stuck");
        await Wait.Until(() => ActiveHandlers == 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var stop = () => source.StopConsuming(cts.Token);

        await stop.Should().ThrowAsync<TaskCanceledException>();

        handlerGate.TrySetResult();
    }

    [Test]
    public async Task Handler_exception_does_not_stop_the_worker()
    {
        this.messageConsumer.Callback = context =>
            context.Message.Payload == "poison" ? throw new InvalidOperationException("boom") : Task.CompletedTask;

        var source = await StartSource();

        this.consumer.Enqueue(Topic, 1, "poison");
        this.consumer.Enqueue(Topic, 2, "ok");

        await Wait.Until(() => this.messageConsumer.Handled.Count == 2);
        await source.StopConsuming(CancellationToken.None);

        this.messageConsumer.Handled.Select(m => m.Payload).Should().BeEquivalentTo(["poison", "ok"]);
    }

    [Test]
    public async Task StartConsuming_is_idempotent()
    {
        var source = await StartSource();
        await Wait.Until(() => this.factory.Created.Count >= 1);

        await source.StartConsuming(CancellationToken.None);
        await Task.Delay(200);

        this.factory.Created.Should().ContainSingle();

        await source.StopConsuming(CancellationToken.None);
    }
}
