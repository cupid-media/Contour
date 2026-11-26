using Contour.Kafka.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Specific;

/// <summary>
/// Two bus instances in one consumer group split the partitions between themselves,
/// so every message is delivered exactly once across the group.
/// </summary>
public class When_multiple_buses_share_consumer_group : KafkaIntegrationTestBase
{
    private const int Partitions = 2;
    private const int MessagesPerPartition = 3;
    private const int TotalMessages = Partitions * MessagesPerPartition;

    private TestKafkaConsumer<DummyMessage> firstConsumer = null!;
    private TestKafkaConsumer<DummyMessage> secondConsumer = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var topic = await CreateTopic(Partitions);

        this.firstConsumer = new TestKafkaConsumer<DummyMessage>();
        this.secondConsumer = new TestKafkaConsumer<DummyMessage>();

        await CreateAndStartBus(cfg => cfg.On(topic).ReactWith(this.firstConsumer));
        await CreateAndStartBus(cfg => cfg.On(topic).ReactWith(this.secondConsumer));

        // Публикуем только после того, как ребаланс развёл партиции по обоим участникам группы.
        (await WaitForGroupAssignment(DefaultGroupId, expectedMembers: 2))
            .Should().BeTrue("both buses must join the consumer group and get a partition");

        for (var partition = 0; partition < Partitions; partition++)
        {
            await Produce(
                topic,
                Enumerable.Range(0, MessagesPerPartition).Select(i => new DummyMessage { Id = (partition * MessagesPerPartition) + i }),
                partition);
        }

        await WaitForCondition(() => this.firstConsumer.HandledCount + this.secondConsumer.HandledCount == TotalMessages);
    }

    [Test]
    public void Then_every_message_is_delivered_once_across_the_group()
    {
        var handled = this.firstConsumer.Handled
            .Concat(this.secondConsumer.Handled)
            .Select(m => m.Payload.Id)
            .ToList();

        handled.Should().HaveCount(TotalMessages);
        handled.Should().OnlyHaveUniqueItems();
        handled.Should().BeEquivalentTo(Enumerable.Range(0, TotalMessages));
    }

    [Test]
    public void Then_both_buses_get_a_share_of_the_partitions()
    {
        this.firstConsumer.HandledCount.Should().BeGreaterThan(0);
        this.secondConsumer.HandledCount.Should().BeGreaterThan(0);
    }
}
