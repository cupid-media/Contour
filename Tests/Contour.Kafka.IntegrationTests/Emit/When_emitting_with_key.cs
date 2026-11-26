using Confluent.Kafka;
using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Sending;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Emit;

/// <summary>
/// PublishingOptions.Key becomes the Kafka message key: messages with one key share a partition.
/// </summary>
public class When_emitting_with_key : KafkaIntegrationTestBase
{
    private const int MessagesPerKey = 10;

    private List<ConsumeResult<string, byte[]>> consumed = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var topic = await CreateTopic(partitions: 4);

        var bus = await CreateAndStartBus(cfg => cfg.Route(topic));

        for (var i = 0; i < MessagesPerKey; i++)
        {
            foreach (var key in new[] { "chat-a", "chat-b" })
            {
                await bus.EmitAsync(topic, new DummyMessage { Id = i }, new PublishingOptions { Key = key });
            }
        }

        await bus.EmitAsync(topic, new DummyMessage { Id = -1 });

        this.consumed = ConsumeAll(topic, 2 * MessagesPerKey + 1);
    }

    [Test]
    public void Then_messages_with_one_key_land_in_one_partition()
    {
        foreach (var key in new[] { "chat-a", "chat-b" })
        {
            var partitions = this.consumed.Where(r => r.Message.Key == key).Select(r => r.Partition.Value).ToList();

            partitions.Should().HaveCount(MessagesPerKey);
            partitions.Distinct().Should().ContainSingle();
        }
    }

    [Test]
    public void Then_a_message_without_a_key_has_no_key()
    {
        this.consumed.Where(r => r.Message.Key == null).Should().ContainSingle();
    }

    private static List<ConsumeResult<string, byte[]>> ConsumeAll(string topic, int count)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"raw-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest
        }).Build();

        consumer.Subscribe(topic);

        var results = new List<ConsumeResult<string, byte[]>>();
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (results.Count < count && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));

            if (result != null)
            {
                results.Add(result);
            }
        }

        consumer.Close();

        return results;
    }
}
