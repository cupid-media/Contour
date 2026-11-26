using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.BusLifecycle;

public class When_bus_created_but_not_started : KafkaIntegrationTestBase
{
    private IBus bus = null!;
    private TestKafkaConsumer<DummyMessage> consumer = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        this.bus = await CreateBus(cfg => cfg.On(topic).ReactWith(this.consumer));

        await Produce(topic, new DummyMessage { Id = 1 });
        await Task.Delay(TimeSpan.FromSeconds(2));
    }

    [Test]
    public void Then_bus_should_not_be_initialized()
    {
        this.bus.State.Should().Be(BusState.NotInitialized);
        this.bus.IsReady.Should().BeFalse();
    }

    [Test]
    public void Then_bus_should_not_be_healthy()
    {
        this.bus.IsHealthy.Should().BeFalse();
    }

    [Test]
    public void Then_no_messages_are_consumed()
    {
        this.consumer.HandledCount.Should().Be(0);
    }
}
