using Contour.RabbitMQ.IntegrationTests.Helpers;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_emitted_message_doesnot_have_props : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;

    private Message<DummyMessage> incomingMessage;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testIncomingQueueName = GetRandomQueueName();

        await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testIncomingQueueName)
                .ReactWith(new TestExtendedConsumer<DummyMessage>(async context =>
                {
                    this.incomingMessage = context.Message;
                    await context.AcceptAsync();
                }))
                .RequiresAccept();
        });

        await ManagementClient.PublishAsync(new ExchangeName(this.testIncomingQueueName, "/"), new PublishInfo("", @"{ ""Id"": 10 }"));

        await Task.Delay(1000);
    }

    [Test]
    public void Then_message_payload_is_populated()
    {
        this.incomingMessage.Payload.Should().NotBeNull();
        this.incomingMessage.Payload.Id.Should().Be(10);
    }

    [Test]
    public void Then_message_label_is_set_from_queue_name()
    {
        this.incomingMessage.Label.Name.Should().Be(this.testIncomingQueueName);
    }

    [Test]
    public void Then_message_headers_are_empty()
    {
        this.incomingMessage.Headers.Should().BeEmpty();
    }
}
