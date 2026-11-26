using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.BusLifecycle;

public class When_bus_started_and_then_stopped : RabbitIntegrationTestBase
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
        await Task.Delay(3000);
        await this.bus.Stop();
    }

    [Test]
    public void Then_bus_should_be_stopped()
    {
        this.bus.State.Should().Be(BusState.Stopped);
    }

    [Test]
    public async Task Then_queue_exists_without_consumers()
    {
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.queueName))?.Consumers == 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.queueName);
        queue.Should().NotBeNull();
        queue.Consumers.Should().Be(0);
    }
}
