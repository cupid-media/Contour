using System.Text.Json;
using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Fault;

/// <summary>
/// A handler that throws: the message is forwarded to the fault topic and its offset advances,
/// so a restarted bus does not read it again.
/// </summary>
public class When_handler_fails_with_fault_strategy : KafkaIntegrationTestBase
{
    private TestKafkaConsumer<DummyMessage> retryConsumer = null!;
    private string topic = null!;
    private string faultTopic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        // Fault-топик производится шиной от имени конечной точки.
        this.faultTopic = await CreateTopic($"{DefaultEndpointName}.fault".ToLowerInvariant());

        var failing = new TestKafkaConsumer<DummyMessage>(_ => throw new InvalidOperationException("boom"));

        var bus = await CreateAndStartBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(this.topic).ReactWith(failing);
        });

        await Produce(this.topic, new DummyMessage { Id = 13 });

        await WaitForCondition(() => failing.HandledCount == 1);
        await WaitForCondition(() => FaultMessages().Count == 1);

        await bus.Stop();

        // Новая шина в той же группе: упавшее сообщение уже переложено и не должно вернуться.
        this.retryConsumer = new TestKafkaConsumer<DummyMessage>();
        await CreateAndStartBus(cfg => cfg.On(this.topic).ReactWith(this.retryConsumer));

        await Task.Delay(TimeSpan.FromSeconds(3));
    }

    [Test]
    public void Then_the_failed_message_lands_in_the_fault_topic()
    {
        var fault = FaultMessages().Should().ContainSingle().Subject;

        fault.Label.Should().Be(this.topic);
        fault.Exception.Should().NotBeNull();
        fault.Payload.ToString().Should().Contain("13");
    }

    [Test]
    public void Then_the_offset_is_advanced_past_the_failed_message()
    {
        GetCommittedOffset(DefaultGroupId, this.topic).Should().Be(1);
    }

    [Test]
    public void Then_a_restarted_bus_does_not_read_it_again()
    {
        this.retryConsumer.HandledCount.Should().Be(0);
    }

    /// <summary>Читает fault-топик сырым консьюмером: тип сообщения там свой.</summary>
    private List<KafkaFaultMessage> ReadFaultTopic()
    {
        using var consumer = new Confluent.Kafka.ConsumerBuilder<Confluent.Kafka.Ignore, byte[]>(
            new Confluent.Kafka.ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = $"{DefaultGroupId}-fault-reader-{Guid.NewGuid():N}",
                AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            }).Build();

        consumer.Subscribe(this.faultTopic);

        var messages = new List<KafkaFaultMessage>();

        while (true)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(2));

            if (result == null)
            {
                break;
            }

            messages.Add(JsonSerializer.Deserialize<KafkaFaultMessage>(result.Message.Value)!);
        }

        consumer.Close();

        return messages;
    }

    private List<KafkaFaultMessage> FaultMessages() => ReadFaultTopic();
}
