using Contour.Configuration;
using Contour.RabbitMQ.IntegrationTests.Helpers;
using Contour.Receiving;
using Contour.Transport.RabbitMQ.Topology;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_multiple_receivers_on_same_queue : RabbitIntegrationTestBase
{
    private IBus bus;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var testQueue = GetRandomQueueName();
        var testQueue2 = GetRandomQueueName();
        this.bus = await CreateBus(DefaultEndpointName, cfg =>
        {
            cfg.WithEndpointBuilder(async (builderParams, token) =>
            {
                var queueName = builderParams.Endpoint.Address + "." + testQueue; // all messages go through testQueue
                var queueBuilder = Queue.Named(queueName).Durable();
                var queue = await builderParams.Topology.Declare(queueBuilder, token);

                var exchangeBuilder = Exchange.Named(testQueue).Durable;
                var exchange = await builderParams.Topology.Declare(exchangeBuilder, token);
                await builderParams.Topology.Bind(exchange, queue, cancellationToken: token);

                return new SubscriptionEndpoint(queue, exchange);
            });

            cfg.WithRouteResolverBuilder(async (builderParams, token) =>
            {
                var exchangeBuilder = Exchange.Named(testQueue).Durable; // all messages go through testQueue
                var exchange = await builderParams.Topology.Declare(exchangeBuilder, token);
                return exchange;
            });

            cfg.Route(testQueue)
                .WithDefaultCallbackEndpoint();

            cfg.Route(testQueue2)
                .WithDefaultCallbackEndpoint();

            cfg.On(testQueue).ReactWith(new TestConsumer<DummyMessage, DummyMessage>(m => m));
            cfg.On(testQueue2).ReactWith(new TestConsumer<DummyMessage, DummyMessage>(m => m));
        });
    }

    [Test]
    public async Task Then_bus_is_configured_incorrectly()
    {
        await this.bus.Invoking(b => b.Prepare())
            .Should().ThrowExactlyAsync<BusConfigurationException>();
    }
}
