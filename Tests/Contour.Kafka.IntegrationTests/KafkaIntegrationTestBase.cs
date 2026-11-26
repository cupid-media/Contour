using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Contour.Serialization;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;

namespace Contour.Kafka.IntegrationTests;

/// <summary>
/// Base class for integration tests that provides common setup for Contour with Kafka TestContainer.
/// Inherit from this class to get access to a configured bus factory, a raw producer and offset helpers.
/// </summary>
[TestFixture]
public abstract class KafkaIntegrationTestBase
{
    private readonly List<IBus> createdBuses = [];

    protected static string BootstrapServers => KafkaTestFixture.BootstrapServers;

    protected static IPayloadConverter PayloadConverter { get; } = new JsonPayloadConverter();

    /// <summary>
    /// Consumer group unique per test class: tests must not observe each other's committed offsets.
    /// </summary>
    protected string DefaultGroupId { get; } = $"group-{Guid.NewGuid():N}";

    /// <summary>
    /// Endpoint name of the buses created by this fixture; shows up in the x-breadcrumbs header.
    /// </summary>
    protected string DefaultEndpointName => GetType().Name;

    [OneTimeTearDown]
    public virtual async Task TearDown()
    {
        foreach (var bus in createdBuses)
        {
            await bus.Stop();
        }

        createdBuses.Clear();
    }

    /// <summary>
    /// Creates a bus preconfigured for the test container. Reading starts from the beginning of the
    /// topic so that a test does not depend on when the consumer group finishes joining.
    /// </summary>
    protected async Task<IBus> CreateBus(Action<KafkaBusConfigurator> config, bool autoStart = false, string? groupId = null)
    {
        var bus = await BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.AutoStart = autoStart;

            cfg.WithEndpointName(DefaultEndpointName)
                .WithConnectionString(BootstrapServers)
                .WithGroupId(groupId ?? DefaultGroupId)
                .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                .WithAutoCommit(false)
                .WithPayloadConverter(PayloadConverter);

            config.Invoke(cfg);
        });

        return Track(bus);
    }

    /// <summary>
    /// Registers a bus created outside <see cref="CreateBus"/> so that it is stopped on teardown.
    /// </summary>
    protected IBus Track(IBus bus)
    {
        createdBuses.Add(bus);
        return bus;
    }

    protected async Task<IBus> CreateAndStartBus(Action<KafkaBusConfigurator> config, string? groupId = null)
    {
        var bus = await CreateBus(config, autoStart: false, groupId);
        await bus.Start();
        return bus;
    }

    protected static string GetRandomTopicName() => $"topic-{Guid.NewGuid():N}";

    /// <summary>
    /// Creates a topic up front: relying on auto-creation makes the first poll racy.
    /// </summary>
    protected static Task<string> CreateTopic(int partitions = 1)
    {
        return CreateTopic(GetRandomTopicName(), partitions);
    }

    /// <summary>
    /// Creates a topic with a known name: derived topics, such as the fault one, are named by the bus.
    /// </summary>
    protected static async Task<string> CreateTopic(string topic, int partitions = 1)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }
        ]);

        return topic;
    }

    protected static async Task Produce<T>(string topic, T payload, int? partition = null)
        where T : class
    {
        using var producer = new ProducerBuilder<Null, byte[]>(
            new ProducerConfig { BootstrapServers = BootstrapServers }).Build();

        var message = new Message<Null, byte[]> { Value = PayloadConverter.FromObject(payload) };

        if (partition.HasValue)
        {
            await producer.ProduceAsync(new TopicPartition(topic, new Partition(partition.Value)), message);
        }
        else
        {
            await producer.ProduceAsync(topic, message);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    protected static async Task Produce<T>(string topic, IEnumerable<T> payloads, int? partition = null)
        where T : class
    {
        foreach (var payload in payloads)
        {
            await Produce(topic, payload, partition);
        }
    }

    /// <summary>
    /// Offset committed by the group for the partition, or <c>null</c> when nothing was committed.
    /// </summary>
    protected static long? GetCommittedOffset(string groupId, string topic, int partition = 0)
    {
        using var consumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = groupId
        }).Build();

        var committed = consumer.Committed(
            [new TopicPartition(topic, new Partition(partition))],
            TimeSpan.FromSeconds(10));

        var offset = committed.SingleOrDefault()?.Offset;

        return offset is null || offset == Offset.Unset ? null : offset.Value.Value;
    }

    /// <summary>
    /// Waits until the consumer group has the expected number of members and every one of them
    /// owns at least one partition. Producing before the group has settled makes tests that
    /// assert on the partition split racy.
    /// </summary>
    protected static async Task<bool> WaitForGroupAssignment(string groupId, int expectedMembers, TimeSpan? timeout = null)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

        return await WaitForCondition(
            async () =>
            {
                try
                {
                    var result = await admin.DescribeConsumerGroupsAsync([groupId]);
                    var group = result.ConsumerGroupDescriptions.SingleOrDefault();

                    return group != null
                           && group.Members.Count == expectedMembers
                           && group.Members.All(m => m.Assignment.TopicPartitions.Count > 0);
                }
                catch (KafkaException)
                {
                    return false;
                }
            },
            timeout);
    }

    /// <summary>
    /// Helper method to wait for a condition with timeout
    /// </summary>
    protected static Task<bool> WaitForCondition(Func<bool> condition, TimeSpan? timeout = null, TimeSpan? checkInterval = null)
    {
        return WaitForCondition(() => Task.FromResult(condition()), timeout, checkInterval);
    }

    /// <summary>
    /// Helper method to wait for an asynchronous condition with timeout
    /// </summary>
    protected static async Task<bool> WaitForCondition(Func<Task<bool>> condition, TimeSpan? timeout = null, TimeSpan? checkInterval = null)
    {
        var endTime = DateTime.UtcNow.Add(timeout ?? TimeSpan.FromSeconds(30));

        while (DateTime.UtcNow < endTime)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(checkInterval ?? TimeSpan.FromMilliseconds(100));
        }

        return await condition();
    }
}
