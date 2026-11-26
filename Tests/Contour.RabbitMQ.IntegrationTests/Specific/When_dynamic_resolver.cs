using Contour.RabbitMQ.IntegrationTests.Helpers;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Configuration;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_dynamic_resolver: RabbitIntegrationTestBase
{
    private string testOutgoingQueueName;

    private IBus bus;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testOutgoingQueueName = GetRandomQueueName();

        this.bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(MessageLabel.Any).ConfiguredWith((builder, _) => new ValueTask<IRouteResolver>(new DynamicRouteResolver(builder)));
        });
    }

    [Test]
    public async Task Then_emits_dynamic_route_successfully()
    {
        await this.bus.EmitAsync(this.testOutgoingQueueName, new DummyMessage());
    }
}
