using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Consume;

/// <summary>
/// A stopped bus cannot be restarted — same as RmqBus. A fresh instance in the same consumer
/// group picks up from the offsets committed by the previous one.
/// </summary>
public class When_bus_is_replaced_after_stop : KafkaIntegrationTestBase
{
    private IBus stoppedBus = null!;
    private TestKafkaConsumer<DummyMessage> firstConsumer = null!;
    private TestKafkaConsumer<DummyMessage> secondConsumer = null!;
    private string topic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.firstConsumer = new TestKafkaConsumer<DummyMessage>();

        this.stoppedBus = await CreateAndStartBus(cfg => cfg.On(this.topic).ReactWith(this.firstConsumer));

        await Produce(this.topic, new DummyMessage { Id = 1 });
        await WaitForCondition(() => this.firstConsumer.HandledCount == 1);

        await this.stoppedBus.Stop();
        await this.stoppedBus.Start();

        this.secondConsumer = new TestKafkaConsumer<DummyMessage>();
        await CreateAndStartBus(cfg => cfg.On(this.topic).ReactWith(this.secondConsumer));

        await Produce(this.topic, new DummyMessage { Id = 2 });
        await WaitForCondition(() => this.secondConsumer.HandledCount == 1);
    }

    [Test]
    public void Then_the_stopped_bus_does_not_come_back()
    {
        this.stoppedBus.State.Should().Be(BusState.Stopped);
        this.stoppedBus.IsReady.Should().BeFalse();
    }

    [Test]
    public void Then_the_stopped_bus_consumes_nothing_more()
    {
        this.firstConsumer.Handled.Select(m => m.Payload.Id).Should().Equal(1);
    }

    [Test]
    public void Then_a_new_bus_resumes_from_the_committed_offset()
    {
        // Оффсет первого сообщения зафиксирован при остановке, поэтому повторной доставки нет.
        this.secondConsumer.Handled.Select(m => m.Payload.Id).Should().Equal(2);
    }
}
