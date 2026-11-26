using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_consuming_is_stopped : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;
    private string testRequestQueueName;

    private IBus testBus;
    private IBus helperBus;

    private bool incomingMessageReceived;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testIncomingQueueName = GetRandomQueueName();
        this.testRequestQueueName = GetRandomQueueName();

        this.testBus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testIncomingQueueName)
                .ReactWith(new TestConsumer<DummyMessage>(_ =>
                {
                    this.incomingMessageReceived = true;
                }))
                .RequiresAccept();

            cfg.Route(this.testRequestQueueName)
                .WithDefaultCallbackEndpoint();
        });

        this.helperBus = await CreateAndStartBus(DefaultEndpointName + "2", cfg =>
        {
            cfg.Route(this.testIncomingQueueName);

            cfg.On(this.testRequestQueueName)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(_ => new DummyMessage()))
                .RequiresAccept();
        });

        await this.testBus.StopReceiving();
    }

    [Test]
    public async Task Then_message_is_not_consumed()
    {
        await this.helperBus.EmitAsync(this.testIncomingQueueName, new DummyMessage());
        await Task.Delay(1000);
        this.incomingMessageReceived.Should().BeFalse();
    }

    [Test]
    public async Task Then_bus_can_send_requests()
    {
        var response = await this.testBus.RequestAsync<DummyMessage, DummyMessage>(this.testRequestQueueName, new DummyMessage());
        response.Response.Should().NotBeNull();
    }
}
