using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Emit;

/// <summary>
/// A producing bus and a consuming bus, as two separate services would be deployed.
/// </summary>
public class When_emitting_between_two_buses : KafkaIntegrationTestBase
{
    private const int MessageCount = 5;

    private IBus publisher = null!;
    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        await CreateAndStartBus(cfg => cfg.On(this.topic).ReactWith(this.consumer));

        this.publisher = await CreateAndStartBus(
            cfg => cfg.Route(this.topic),
            groupId: $"{DefaultGroupId}-publisher");

        for (var id = 1; id <= MessageCount; id++)
        {
            await this.publisher.EmitAsync(this.topic, new DummyMessage { Id = id });
        }

        await WaitForCondition(() => this.consumer.HandledCount == MessageCount);
    }

    [Test]
    public void Then_every_emitted_message_is_delivered()
    {
        this.consumer.Handled.Select(m => m.Payload.Id)
            .Should()
            .BeEquivalentTo(Enumerable.Range(1, MessageCount));
    }

    [Test]
    public void Then_publisher_has_no_receivers()
    {
        this.publisher.CanHandle(this.topic).Should().BeFalse();
        this.publisher.ActiveRequestsCount.Should().Be(0);
    }

    [Test]
    public void Then_publisher_is_healthy()
    {
        this.publisher.IsHealthy.Should().BeTrue();
    }
}
