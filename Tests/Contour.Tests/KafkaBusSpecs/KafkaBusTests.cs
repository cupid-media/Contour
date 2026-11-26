using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

[TestFixture]
[Category("Unit")]
public class KafkaBusTests
{
    private const string Topic = "bus-topic";

    private FakeKafkaConsumer consumer;
    private FakeKafkaConsumerFactory factory;
    private TestKafkaConsumerOf<string> messageConsumer;

    [SetUp]
    public void SetUp()
    {
        this.consumer = new FakeKafkaConsumer();
        this.factory = new FakeKafkaConsumerFactory();
        this.factory.Prepare(this.consumer);
        this.messageConsumer = new TestKafkaConsumerOf<string>();
    }

    private Task<IBus> CreateBus()
    {
        return BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ConsumerFactory = this.factory;
            cfg.AutoStart = false;

            cfg.WithConnectionString("fake:9092")
                .WithGroupId("bus-group")
                .WithAutoCommit(false)
                .WithPayloadConverter(new PassThroughPayloadConverter());

            cfg.On(Topic).ReactWith(this.messageConsumer);
        });
    }

    [Test]
    public async Task Start_fails_when_a_receiver_cannot_subscribe()
    {
        var bus = await BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ConsumerFactory = new ThrowingKafkaConsumerFactory(
                new InvalidOperationException("No such configuration property"));
            cfg.AutoStart = false;

            cfg.WithConnectionString("fake:9092")
                .WithGroupId("bus-group")
                .WithPayloadConverter(new PassThroughPayloadConverter());

            cfg.On(Topic).ReactWith(this.messageConsumer);
        });

        // Иначе шина отрапортовала бы успешный старт и молча не читала бы ничего.
        await bus.Invoking(b => b.Start()).Should().ThrowAsync<InvalidOperationException>();

        bus.IsReady.Should().BeFalse();
    }

    [Test]
    public async Task Bus_reports_the_kafka_transport()
    {
        var bus = await CreateBus();

        bus.Transport.Should().Be(BusTransports.Kafka);
    }

    [Test]
    public async Task Start_configures_receivers_and_marks_bus_as_working()
    {
        var bus = await CreateBus();

        bus.State.Should().Be(BusState.NotInitialized);

        await bus.Start();

        bus.State.Should().Be(BusState.Working);
        bus.IsReady.Should().BeTrue();

        // Subscribe выполняется на потоке воркера чуть позже Start — дожидаемся.
        await Wait.Until(() => this.consumer.SubscribedTopics.Contains(Topic));

        await bus.Stop();
    }

    [Test]
    public async Task Start_is_idempotent()
    {
        var bus = await CreateBus();

        await bus.Start();
        await Wait.Until(() => this.factory.Created.Count >= 1);

        await bus.Start();
        await Task.Delay(200);

        this.factory.Created.Should().ContainSingle();

        await bus.Stop();
    }

    [Test]
    public async Task Started_bus_delivers_messages_to_consumer()
    {
        var bus = await CreateBus();
        await bus.Start();

        this.consumer.Enqueue(Topic, 1, "payload");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        await bus.Stop();

        this.messageConsumer.Handled.Should().ContainSingle().Which.Payload.Should().Be("payload");
    }

    [Test]
    public async Task Stop_transitions_to_stopped_and_closes_consumers()
    {
        var bus = await CreateBus();
        await bus.Start();
        await bus.Stop();

        bus.State.Should().Be(BusState.Stopped);
        bus.IsReady.Should().BeFalse();
        this.consumer.IsClosed.Should().BeTrue();
        this.consumer.IsDisposed.Should().BeTrue();
    }

    [Test]
    public async Task Stop_without_start_is_noop()
    {
        var bus = await CreateBus();

        await bus.Stop();

        bus.State.Should().Be(BusState.NotInitialized);
        this.factory.Created.Should().BeEmpty();
    }

    [Test]
    public async Task Lifecycle_events_are_raised_in_order()
    {
        var bus = await CreateBus();
        var events = new List<string>();

        bus.Starting += _ => events.Add("Starting");
        bus.Started += _ => events.Add("Started");
        bus.Stopping += _ => events.Add("Stopping");
        bus.Stopped += _ => events.Add("Stopped");

        await bus.Start();
        await bus.Stop();

        events.Should().Equal("Starting", "Started", "Stopping", "Stopped");
    }

    [Test]
    public async Task Stop_gracefully_waits_for_inflight_handler_and_commits_its_offset()
    {
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        this.messageConsumer.Callback = async _ =>
        {
            handlerEntered.TrySetResult();
            await handlerGate.Task;
        };

        var bus = await CreateBus();
        await bus.Start();

        this.consumer.Enqueue(Topic, 7, "slow");
        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var stopTask = bus.Stop();
        await Task.Delay(200);

        stopTask.IsCompleted.Should().BeFalse("graceful stop must wait for the in-flight handler");
        bus.ActiveRequestsCount.Should().Be(1);

        handlerGate.TrySetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));

        bus.State.Should().Be(BusState.Stopped);
        this.consumer.Commits.Select(c => (long)c.Offset)
            .Should()
            .Contain(8L, "position after the message handled during shutdown must be committed");
        this.consumer.IsClosed.Should().BeTrue();
    }

    [Test]
    public async Task StopReceiving_stops_consuming_but_keeps_bus_alive()
    {
        var bus = await CreateBus();
        await bus.Start();

        this.consumer.Enqueue(Topic, 1, "before-stop");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        await bus.StopReceiving();

        // Как и RmqBus: шина остаётся Working, пока не остановлена и отправка,
        // но получатели уже не работают — это видно по health-проверке.
        bus.State.Should().Be(BusState.Working);
        bus.IsHealthy.Should().BeFalse("receivers are stopped");

        this.consumer.Enqueue(Topic, 2, "after-stop");
        await Task.Delay(300);

        this.messageConsumer.Handled.Should().ContainSingle("no new messages must be consumed after StopReceiving");

        await bus.Stop();
    }

    [Test]
    public async Task WaitForHandlersCompletion_returns_when_no_handlers_are_active()
    {
        var bus = await CreateBus();
        await bus.Start();

        Func<Task> wait = () => bus.WaitForRequestsCompletion(new CancellationTokenSource(TimeSpan.FromSeconds(1)).Token);

        await wait.Should().NotThrowAsync();

        await bus.Stop();
    }

    [Test]
    public async Task Bus_cannot_be_restarted_after_stop()
    {
        var bus = await CreateBus();

        await bus.Start();
        await bus.Stop();

        var secondConsumer = new FakeKafkaConsumer();
        this.factory.Prepare(secondConsumer);

        // Как и RmqBus: остановленная шина не поднимается повторно — нужен новый экземпляр.
        await bus.Start();

        bus.State.Should().Be(BusState.Stopped);
        bus.IsReady.Should().BeFalse();
        this.factory.Created.Should().ContainSingle("no new consumer must be created for a stopped bus");
    }

    [Test]
    public async Task Dispose_stops_the_bus_gracefully()
    {
        var bus = await CreateBus();
        await bus.Start();

        // Приведение намеренное: синхронное освобождение даёт реализация шины, а не сам IBus —
        // интерфейс его не требует, чтобы не ломать сторонние реализации.
        await bus.DisposeAsync();

        bus.State.Should().Be(BusState.Stopped);
        this.consumer.IsClosed.Should().BeTrue();
    }
}
