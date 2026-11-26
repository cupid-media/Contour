using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.BusLifecycle;

public class When_bus_created_and_prepared : RabbitIntegrationTestBase
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
        await this.bus.Prepare();
    }

    [Test]
    public void Then_bus_should_be_configured()
    {
        this.bus.State.Should().Be(BusState.Configured);
    }

    [Test]
    public async Task Then_queue_is_created_without_consumers()
    {
        await WaitForCondition(() => HasQueue(DefaultEndpointName, this.queueName), TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.queueName);
        queue.Should().NotBeNull();
        queue.Consumers.Should().Be(0);
    }
}
