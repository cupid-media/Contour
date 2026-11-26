using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Consume;

public class When_parallelismlevel_specified : KafkaIntegrationTestBase
{
    private const int ParallelismLevel = 3;

    private readonly TaskCompletionSource handlersGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int concurrentHandlers;
    private int peakConcurrentHandlers;

    private IBus bus = null!;
    private TestKafkaConsumer<DummyMessage> consumer = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var topic = await CreateTopic(partitions: ParallelismLevel);

        this.consumer = new TestKafkaConsumer<DummyMessage>(async _ =>
        {
            var current = Interlocked.Increment(ref this.concurrentHandlers);
            InterlockedMax(ref this.peakConcurrentHandlers, current);

            // Держим обработчики занятыми, пока все воркеры не заберут по сообщению.
            await Task.WhenAny(this.handlersGate.Task, Task.Delay(TimeSpan.FromSeconds(30)));

            Interlocked.Decrement(ref this.concurrentHandlers);
        });

        this.bus = await CreateAndStartBus(cfg =>
        {
            cfg.WithParallelismLevel(ParallelismLevel);
            cfg.On(topic).ReactWith(this.consumer);
        });

        // Воркер обрабатывает сообщения по одному и, пока обработчик занят, не опрашивает брокер.
        // Если первый воркер заберёт сообщение до того, как группа поделит партиции, ребаланс
        // будет ждать его, и остальные воркеры партиций не получат.
        (await WaitForGroupAssignment(DefaultGroupId, ParallelismLevel))
            .Should().BeTrue("every worker should own a partition before messages are produced");

        for (var partition = 0; partition < ParallelismLevel; partition++)
        {
            await Produce(topic, new DummyMessage { Id = partition }, partition);
        }

        (await WaitForCondition(() => Volatile.Read(ref this.peakConcurrentHandlers) >= ParallelismLevel))
            .Should().BeTrue("every worker should pick up a message");
    }

    [OneTimeTearDown]
    public override async Task TearDown()
    {
        this.handlersGate.TrySetResult();
        await base.TearDown();
    }

    [Test]
    public void Then_every_worker_handles_its_partition_in_parallel()
    {
        this.peakConcurrentHandlers.Should().Be(ParallelismLevel);
    }

    [Test]
    public void Then_bus_reports_active_handlers()
    {
        this.bus.ActiveRequestsCount.Should().Be(ParallelismLevel);
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)))
        {
            if (Interlocked.CompareExchange(ref target, value, current) == current)
            {
                return;
            }
        }
    }
}
