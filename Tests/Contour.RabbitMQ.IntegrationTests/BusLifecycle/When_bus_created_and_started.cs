using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.BusLifecycle;

public class When_bus_created_and_started : RabbitIntegrationTestBase
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
        await this.bus.Start();
    }

    [Test]
    public void Then_bus_should_be_started()
    {
        this.bus.State.Should().Be(BusState.Working);
    }

    [Test]
    public async Task Then_queue_is_created_with_consumers()
    {
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.queueName))?.Consumers > 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.queueName);
        queue.Should().NotBeNull();
        queue.Consumers.Should().Be(1);
    }
}
