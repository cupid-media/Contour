using Contour.RabbitMQ.IntegrationTests.Helpers;
using Contour.Sending;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Request;

public class When_multiple_requests_from_one_consume : RabbitIntegrationTestBase
{
    private readonly List<Message<DummyMessage>> receivedMessages = new();
    private readonly List<RequestResult<DummyMessage>> responses = new();

    [OneTimeSetUp]
    public async Task Setup()
    {
        var testIncomingQueue = GetRandomQueueName();
        var testRequestingQueue = GetRandomQueueName();

        IBus bus = null;
        bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(testIncomingQueue)
                .WithDefaultCallbackEndpoint();

            cfg.Route(testRequestingQueue)
                .WithDefaultCallbackEndpoint();

            cfg.On(testIncomingQueue)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(async m =>
                {
                    this.responses.Add(await bus.RequestAsync<DummyMessage, DummyMessage>(testRequestingQueue, new DummyMessage()));
                    this.responses.Add(await bus.RequestAsync<DummyMessage, DummyMessage>(testRequestingQueue, new DummyMessage()));
                    this.responses.Add(await bus.RequestAsync<DummyMessage, DummyMessage>(testRequestingQueue, new DummyMessage()));
                    return new DummyMessage();
                }));

            cfg.On(testRequestingQueue)
                .ReactWith(new TestExtendedConsumer<DummyMessage>(async context =>
                {
                    this.receivedMessages.Add(context.Message);
                    await context.AcceptAsync();
                    await context.ReplyAsync(new DummyMessage());
                }));
        });

        await bus.RequestAsync<DummyMessage, DummyMessage>(testIncomingQueue, new DummyMessage());
    }

    [Test]
    public void Then_all_received_messages_have_unique_correlationId()
    {
        this.receivedMessages.Should().HaveCount(3);
        this.receivedMessages.Select(m => HeadersHelper.ReadHeaderValue(m.Headers[Headers.CorrelationId])).Distinct().Should().HaveCount(3);
    }

    [Test]
    public void Then_all_responses_received()
    {
        this.responses.Should().HaveCount(3);
        this.responses.Select(r => r.Response).Should().AllSatisfy(r => r.Should().NotBeNull());
    }
}
