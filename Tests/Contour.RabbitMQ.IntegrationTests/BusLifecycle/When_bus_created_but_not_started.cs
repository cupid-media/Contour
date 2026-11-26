using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.BusLifecycle;

public class When_bus_created_but_not_started : RabbitIntegrationTestBase
{
    private IBus bus;
    private string queueName;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.queueName = GetRandomQueueName();
        this.bus = await CreateBus(DefaultEndpointName, c =>
        {
            c.On(this.queueName)
                .ReactWith(new TestConsumer<DummyMessage>(_ => {}));
        });
    }

    [Test]
    public void Then_bus_not_initialized()
    {
        this.bus.State.Should().Be(BusState.NotInitialized);
    }

    [Test]
    public async Task Then_queue_is_not_created()
    {
        await Task.Delay(3000);
        var hasQueue = await HasQueue(DefaultEndpointName, this.queueName);
        hasQueue.Should().BeFalse();
    }
}
