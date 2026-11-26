using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Specific;

public class When_handler_throws : KafkaIntegrationTestBase
{
    private IBus bus = null!;
    private TestKafkaConsumer<DummyMessage> consumer = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var topic = await CreateTopic();

        this.consumer = new TestKafkaConsumer<DummyMessage>(message =>
        {
            if (message.Payload.Id == 1)
            {
                throw new InvalidOperationException("boom");
            }
        });

        this.bus = await CreateAndStartBus(cfg => cfg.On(topic).ReactWith(this.consumer));

        await Produce(topic, new DummyMessage { Id = 1 });
        await Produce(topic, new DummyMessage { Id = 2 });

        await WaitForCondition(() => this.consumer.HandledCount == 2);
    }

    [Test]
    public void Then_the_worker_survives_and_keeps_consuming()
    {
        this.consumer.Handled.Select(m => m.Payload.Id).Should().Equal(1, 2);
    }

    [Test]
    public void Then_bus_stays_healthy()
    {
        this.bus.IsHealthy.Should().BeTrue();
    }
}
