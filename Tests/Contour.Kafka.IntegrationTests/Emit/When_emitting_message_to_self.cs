using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Sending;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Emit;

/// <summary>
/// The bus both routes to and consumes from the same topic: a full emit → consume round trip.
/// </summary>
public class When_emitting_message_to_self : KafkaIntegrationTestBase
{
    private IBus bus = null!;
    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        this.bus = await CreateAndStartBus(cfg =>
        {
            cfg.Route(this.topic);
            cfg.On(this.topic).ReactWith(this.consumer);
        });

        await this.bus.EmitAsync(this.topic, new DummyMessage { Id = 42 });

        await this.bus.EmitAsync(this.topic, new DummyMessage { Id = 7 }, new PublishingOptions
        {
            AdditionalHeaders = new() { ["x-tenant"] = "acme" }
        });

        await WaitForCondition(() => this.consumer.HandledCount == 2);
    }

    [Test]
    public void Then_messages_are_received_and_deserialized()
    {
        this.consumer.Handled.Select(m => m.Payload.Id).Should().BeEquivalentTo([42, 7]);
        this.consumer.Handled.Should().OnlyContain(m => m.Label.Name == this.topic);
    }

    [Test]
    public void Then_contour_headers_travel_with_the_message()
    {
        var message = this.consumer.Handled.Single(m => m.Payload.Id == 42);

        message.Headers.Should().ContainKey(Headers.CorrelationId);
        message.Headers[Headers.Breadcrumbs].Should().Be(DefaultEndpointName);
    }

    [Test]
    public void Then_additional_headers_are_delivered()
    {
        var message = this.consumer.Handled.Single(m => m.Payload.Id == 7);

        message.Headers["x-tenant"].Should().Be("acme");
    }

    [Test]
    public void Then_bus_reports_the_route_and_the_handler()
    {
        this.bus.CanRoute(this.topic).Should().BeTrue();
        this.bus.CanHandle(this.topic).Should().BeTrue();
    }
}
