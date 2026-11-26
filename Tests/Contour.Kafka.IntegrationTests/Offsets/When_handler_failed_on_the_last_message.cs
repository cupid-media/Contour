using Contour.Kafka.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Offsets;

/// <summary>
/// A message whose handler threw and whose fault strategy rejected it with requeue must not be
/// committed: it has to be redelivered to the next instance of the same consumer group.
/// </summary>
public class When_handler_failed_on_the_last_message : KafkaIntegrationTestBase
{
    private TestKafkaConsumer<DummyMessage> failingConsumer = null!;
    private TestKafkaConsumer<DummyMessage> retryingConsumer = null!;
    private long? committedAfterFailure;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var topic = await CreateTopic();

        this.failingConsumer = new TestKafkaConsumer<DummyMessage>(
            _ => throw new InvalidOperationException("boom"));

        // Без стратегии упавшее сообщение пропускается; удержать его оффсет может только стратегия.
        var bus = await CreateAndStartBus(cfg => cfg.On(topic)
            .OnFailed(context => context.RejectAsync(requeue: true))
            .ReactWith(this.failingConsumer));

        await Produce(topic, new DummyMessage { Id = 1 });
        await WaitForCondition(() => this.failingConsumer.HandledCount == 1);

        await bus.Stop();

        // Снимок до старта второй шины: та обработает сообщение успешно и закоммитит его.
        this.committedAfterFailure = GetCommittedOffset(DefaultGroupId, topic);

        this.retryingConsumer = new TestKafkaConsumer<DummyMessage>();
        await CreateAndStartBus(cfg => cfg.On(topic).ReactWith(this.retryingConsumer));

        await WaitForCondition(() => this.retryingConsumer.HandledCount == 1);
    }

    [Test]
    public void Then_offset_of_the_failed_message_is_not_committed()
    {
        this.committedAfterFailure.Should().BeNull();
    }

    [Test]
    public void Then_message_is_redelivered_after_restart()
    {
        this.retryingConsumer.Handled.Should().ContainSingle().Which.Payload.Id.Should().Be(1);
    }
}
