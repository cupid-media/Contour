using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Request;

public class When_requesting_message_from_multiple_sources : RabbitIntegrationTestBase
{
    private bool requested1;
    private bool requested2;

    private DummyMessage response;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var testQueue = GetRandomQueueName();
        var publisher = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(testQueue)
                .WithDefaultCallbackEndpoint();
        });

        await CreateAndStartBus(DefaultEndpointName + "1", cfg =>
        {
            cfg.On(testQueue)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(m =>
                {
                    this.requested1 = true;
                    return new DummyMessage
                    {
                        Id = 1
                    };
                }));
        });

        await CreateAndStartBus(DefaultEndpointName + "2", cfg =>
        {
            cfg.On(testQueue)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(m =>
                {
                    this.requested2 = true;
                    return new DummyMessage
                    {
                        Id = 2
                    };
                }));
        });

        this.response = (await publisher.RequestAsync<DummyMessage, DummyMessage>(testQueue, new DummyMessage())).Response;
        await Task.Delay(100);
    }

    [Test]
    public void Then_message_is_requested_in_all_receivers()
    {
        this.requested1.Should().BeTrue();
        this.requested2.Should().BeTrue();
    }

    [Test]
    public void Then_response_is_received_and_one_is()
    {
        this.response.Id.Should().BeOneOf(1, 2);
    }
}
