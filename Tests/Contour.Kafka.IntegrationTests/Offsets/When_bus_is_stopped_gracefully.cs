using Contour.Kafka.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Offsets;

public class When_bus_is_stopped_gracefully : KafkaIntegrationTestBase
{
    private const int MessageCount = 3;

    private readonly TaskCompletionSource handlerEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource handlerGate = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;
    private Task stopTask = null!;
    private bool stopWasPendingWhileHandlerWasRunning;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();

        this.consumer = new TestKafkaConsumer<DummyMessage>(async message =>
        {
            if (message.Payload.Id != MessageCount)
            {
                return;
            }

            // Последнее сообщение «зависает» в обработчике на момент остановки шины.
            this.handlerEntered.TrySetResult();
            await this.handlerGate.Task;
        });

        var bus = await CreateAndStartBus(cfg => cfg.On(this.topic).ReactWith(this.consumer));

        await Produce(this.topic, Enumerable.Range(1, MessageCount).Select(id => new DummyMessage { Id = id }));
        await this.handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        this.stopTask = bus.Stop();
        await Task.Delay(TimeSpan.FromSeconds(1));
        this.stopWasPendingWhileHandlerWasRunning = !this.stopTask.IsCompleted;

        this.handlerGate.TrySetResult();
        await this.stopTask.WaitAsync(TimeSpan.FromSeconds(30));
    }

    [Test]
    public void Then_stop_waits_for_the_inflight_handler()
    {
        this.stopWasPendingWhileHandlerWasRunning.Should().BeTrue();
    }

    [Test]
    public void Then_all_messages_are_handled()
    {
        this.consumer.HandledCount.Should().Be(MessageCount);
    }

    [Test]
    public void Then_position_after_the_last_handled_message_is_committed()
    {
        GetCommittedOffset(DefaultGroupId, this.topic).Should().Be(MessageCount);
    }
}
