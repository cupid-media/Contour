using Contour.Kafka.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Consume;

public class When_consuming_multiple_topics : KafkaIntegrationTestBase
{
    private TestKafkaConsumer<DummyMessage> firstConsumer = null!;
    private TestKafkaConsumer<DummyMessage> secondConsumer = null!;
    private string firstTopic = null!;
    private string secondTopic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.firstTopic = await CreateTopic();
        this.secondTopic = await CreateTopic();

        this.firstConsumer = new TestKafkaConsumer<DummyMessage>();
        this.secondConsumer = new TestKafkaConsumer<DummyMessage>();

        await CreateAndStartBus(cfg =>
        {
            cfg.On(this.firstTopic).ReactWith(this.firstConsumer);
            cfg.On(this.secondTopic).ReactWith(this.secondConsumer);
        });

        await Produce(this.firstTopic, new DummyMessage { Id = 1 });
        await Produce(this.secondTopic, new DummyMessage { Id = 2 });

        await WaitForCondition(() => this.firstConsumer.HandledCount == 1 && this.secondConsumer.HandledCount == 1);
    }

    [Test]
    public void Then_every_topic_is_handled_by_its_own_consumer()
    {
        this.firstConsumer.Handled.Should().ContainSingle().Which.Payload.Id.Should().Be(1);
        this.secondConsumer.Handled.Should().ContainSingle().Which.Payload.Id.Should().Be(2);
    }

    [Test]
    public void Then_message_carries_its_topic()
    {
        this.firstConsumer.Handled.Single().Label.Name.Should().Be(this.firstTopic);
        this.secondConsumer.Handled.Single().Label.Name.Should().Be(this.secondTopic);
    }
}
