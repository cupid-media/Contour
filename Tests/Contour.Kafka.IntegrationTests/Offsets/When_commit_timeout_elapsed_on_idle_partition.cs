using Contour.Kafka.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Offsets;

public class When_commit_timeout_elapsed_on_idle_partition : KafkaIntegrationTestBase
{
    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        await CreateAndStartBus(cfg =>
        {
            // Чекпоинт по количеству недостижим: хвост должен быть зафиксирован по таймеру.
            cfg.WithCommitOffset(10_000);
            cfg.WithCommitTimeout(TimeSpan.FromMilliseconds(500));
            cfg.On(this.topic).ReactWith(this.consumer);
        });

        await Produce(this.topic, new DummyMessage { Id = 1 });
        await WaitForCondition(() => this.consumer.HandledCount == 1);
    }

    [Test]
    public async Task Then_pending_tail_is_committed_by_timeout()
    {
        long? committed = null;

        await WaitForCondition(
            () => (committed = GetCommittedOffset(DefaultGroupId, this.topic)) == 1,
            TimeSpan.FromSeconds(30));

        committed.Should().Be(1, "the tail of an idle partition must not stay uncommitted");
    }
}
