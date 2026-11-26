using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.BusLifecycle;

public class When_bus_created_and_started : KafkaIntegrationTestBase
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

        await Produce(this.topic, new DummyMessage { Id = 42 });
        await WaitForCondition(() => this.consumer.HandledCount == 1);
    }

    [Test]
    public void Then_bus_should_be_started()
    {
        this.bus.State.Should().Be(BusState.Working);
        this.bus.IsReady.Should().BeTrue();
    }

    [Test]
    public void Then_bus_should_be_healthy()
    {
        this.bus.IsHealthy.Should().BeTrue();
    }

    [Test]
    public void Then_message_is_received_and_deserialized()
    {
        this.consumer.Handled.Should().ContainSingle();

        var message = this.consumer.Handled.Single();
        message.Label.Name.Should().Be(this.topic);
        message.Payload.Id.Should().Be(42);
    }

    [Test]
    public void Then_no_handlers_are_left_active()
    {
        this.bus.ActiveRequestsCount.Should().Be(0);
    }
}
