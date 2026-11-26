using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Request;

public class When_requesting_message_from_self : RabbitIntegrationTestBase
{
    private bool requested;

    private DummyMessage response;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var testQueue = GetRandomQueueName();
        var bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(testQueue)
                .WithDefaultCallbackEndpoint();

            cfg.On(testQueue)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(m =>
                {
                    this.requested = true;
                    return m;
                }));
        });

        this.response = (await bus.RequestAsync<DummyMessage, DummyMessage>(testQueue, new DummyMessage
        {
            Id = 100500
        })).Response;
    }

    [Test]
    public void Then_message_is_requested()
    {
        this.requested.Should().BeTrue();
    }

    [Test]
    public void Then_response_is_received()
    {
        this.response.Id.Should().Be(100500);
    }
}
