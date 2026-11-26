using Contour.RabbitMQ.IntegrationTests.Helpers;
using Contour.Sending;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

/// <summary>
/// Bus1 publishes to Bus2 with headers. Bus2 while consuming publishes to Bus3
/// </summary>
public class When_incoming_message_has_custom_headers : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;
    private string testOutgoingQueueName;

    private Dictionary<string, object> sentHeaders;
    private IDictionary<string, object> retranslatedHeaders;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testIncomingQueueName = GetRandomQueueName();
        this.testOutgoingQueueName = GetRandomQueueName();

        var bus1 = await CreateAndStartBus(DefaultEndpointName + "1", cfg =>
        {
            cfg.Route(this.testIncomingQueueName);
        });

        IBus bus2 = null;
        bus2 = await CreateAndStartBus(DefaultEndpointName + "2", cfg =>
        {
            cfg.WithExcludedHeaders(["custom-exclude-header"]);
            cfg.On(this.testIncomingQueueName)
                .ReactWith(new TestConsumer<DummyMessage>(async _ =>
                {
                    await bus2!.EmitAsync(this.testOutgoingQueueName, new DummyMessage
                    {
                        Id = 2
                    });
                }));

            cfg.Route(this.testOutgoingQueueName);
        });

        var bus3 = await CreateAndStartBus(DefaultEndpointName + "3", cfg =>
        {
            cfg.On(this.testOutgoingQueueName)
                .ReactWith(new TestExtendedConsumer<DummyMessage>(async m =>
                {
                    this.retranslatedHeaders = m.Message.Headers;
                    await m.AcceptAsync();
                }));
        });

        this.sentHeaders = new Dictionary<string, object>
        {
            {"key1", "value1"},
            {"key2", "value2"},
            {"x-original-message-id", "will-be-overriden"},
            {"custom-exclude-header", "will-be-removed"}
        };
        await bus1.EmitAsync(this.testIncomingQueueName, new DummyMessage(), new PublishingOptions
        {
            AdditionalHeaders = this.sentHeaders
        });

        await Task.Delay(1000);
    }

    [Test]
    public void Then_retranslated_headers_should_be_filled()
    {
        this.retranslatedHeaders.Should().HaveCount(7);
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["key1"]).Should().Be("value1");
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["key2"]).Should().Be("value2");
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["x-breadcrumbs"]).Should().Be("When_incoming_message_has_custom_headers1;When_incoming_message_has_custom_headers2");
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["x-message-type"]).Should().Be(this.testOutgoingQueueName);
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["x-original-message-id"]).Should().NotBe("will-be-overriden");
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["x-correlation-id"]).Should().NotBeNull();
        HeadersHelper.ReadHeaderValue(this.retranslatedHeaders["x-sent-timestamp"]).Should().NotBeNull();
    }
}
