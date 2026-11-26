using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_consuming_is_stopping_while_processing_request : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;

    private IBus testBus;

    private readonly TaskCompletionSource messageReceivedTcs = new();

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testIncomingQueueName = GetRandomQueueName();

        this.testBus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testIncomingQueueName)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(async _ =>
                {
                    this.messageReceivedTcs.SetResult();
                    await Task.Delay(2000);
                    return new DummyMessage();
                }))
                .RequiresAccept();

            cfg.Route(this.testIncomingQueueName)
                .WithDefaultCallbackEndpoint();
        });
    }

    [Test]
    public async Task Then_message_is_processed_and_response_returned()
    {
        var requestTask = this.testBus.RequestAsync<DummyMessage, DummyMessage>(this.testIncomingQueueName, new DummyMessage());
        await this.messageReceivedTcs.Task;
        await this.testBus.StopReceiving();

        this.testBus.ActiveRequestsCount.Should().Be(1);
        await this.testBus.WaitForRequestsCompletion();
        this.testBus.ActiveRequestsCount.Should().Be(0);

        var response = await requestTask;
        response.Response.Should().NotBeNull();
    }
}
