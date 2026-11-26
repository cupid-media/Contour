using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Specific;

public class When_consuming_is_stopped : KafkaIntegrationTestBase
{
    private IBus bus = null!;
    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        this.bus = await CreateAndStartBus(cfg => cfg.On(this.topic).ReactWith(this.consumer));

        await Produce(this.topic, new DummyMessage { Id = 1 });
        await WaitForCondition(() => this.consumer.HandledCount == 1);

        await this.bus.StopReceiving();
    }

    [Test]
    public void Then_bus_stays_working_but_is_no_longer_healthy()
    {
        // Как и RmqBus: шина остаётся Working, пока не остановлена и отправка.
        this.bus.State.Should().Be(BusState.Working);
        this.bus.IsHealthy.Should().BeFalse("receivers are stopped");
    }

    [Test]
    public async Task Then_new_messages_are_not_consumed()
    {
        await Produce(this.topic, new DummyMessage { Id = 2 });
        await Task.Delay(TimeSpan.FromSeconds(3));

        this.consumer.HandledCount.Should().Be(1);
    }

    [Test]
    public async Task Then_waiting_for_handlers_completes_immediately()
    {
        await this.bus.Invoking(b => b.WaitForRequestsCompletion(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token))
            .Should()
            .NotThrowAsync();

        this.bus.ActiveRequestsCount.Should().Be(0);
    }
}
