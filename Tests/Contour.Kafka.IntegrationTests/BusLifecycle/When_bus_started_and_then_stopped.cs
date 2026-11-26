using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.BusLifecycle;

public class When_bus_started_and_then_stopped : KafkaIntegrationTestBase
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

        await this.bus.Stop();
    }

    [Test]
    public void Then_bus_should_be_stopped()
    {
        this.bus.State.Should().Be(BusState.Stopped);
        this.bus.IsReady.Should().BeFalse();
        this.bus.IsHealthy.Should().BeFalse();
    }

    [Test]
    public async Task Then_new_messages_are_not_consumed()
    {
        await Produce(this.topic, new DummyMessage { Id = 2 });
        await Task.Delay(TimeSpan.FromSeconds(3));

        this.consumer.HandledCount.Should().Be(1);
    }

    [Test]
    public void Then_bus_can_be_stopped_again()
    {
        this.bus.Invoking(b => b.Stop()).Should().NotThrowAsync();
    }
}
