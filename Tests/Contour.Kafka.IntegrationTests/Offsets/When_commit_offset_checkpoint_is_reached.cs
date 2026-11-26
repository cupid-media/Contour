using Contour.Kafka.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Offsets;

public class When_commit_offset_checkpoint_is_reached : KafkaIntegrationTestBase
{
    private const int CommitOffset = 2;
    private const int MessageCount = 4;

    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        await CreateAndStartBus(cfg =>
        {
            cfg.WithCommitOffset(CommitOffset);
            // Таймерный коммит отодвинут, чтобы чекпоинт сработал именно по числу сообщений.
            cfg.WithCommitTimeout(TimeSpan.FromMinutes(5));
            cfg.On(this.topic).ReactWith(this.consumer);
        });

        await Produce(this.topic, Enumerable.Range(1, MessageCount).Select(id => new DummyMessage { Id = id }));
        await WaitForCondition(() => this.consumer.HandledCount == MessageCount);
    }

    [Test]
    public async Task Then_offsets_are_committed_without_stopping_the_bus()
    {
        var committed = await WaitForCommittedOffset(MessageCount);

        committed.Should().Be(MessageCount, "checkpoint fires after every {0} handled messages", CommitOffset);
    }

    private async Task<long?> WaitForCommittedOffset(long expected)
    {
        long? committed = null;

        await WaitForCondition(
            () => (committed = GetCommittedOffset(DefaultGroupId, this.topic)) == expected,
            TimeSpan.FromSeconds(30));

        return committed;
    }
}
