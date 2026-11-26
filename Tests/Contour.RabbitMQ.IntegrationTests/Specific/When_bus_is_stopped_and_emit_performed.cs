using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_bus_is_stopped_and_emit_performed : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;

    private IBus bus;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testIncomingQueueName = GetRandomQueueName();

        this.bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(this.testIncomingQueueName);
        });

        await this.bus.Stop();
    }

    [Test]
    public async Task Then_bus_not_ready_exception_is_thrown()
    {
        await this.bus.Invoking(b => b.EmitAsync(this.testIncomingQueueName, new DummyMessage()))
            .Should()
            .ThrowAsync<BusNotReadyException>();
    }
}
