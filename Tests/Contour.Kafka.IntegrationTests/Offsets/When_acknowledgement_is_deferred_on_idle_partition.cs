using System.Collections.Concurrent;
using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Offsets;

/// <summary>
/// A batching handler accepts messages after HandleAsync has returned, when the partition is already
/// quiet: the tail must still be committed by CommitTimeout, not wait for the next message.
/// </summary>
public class When_acknowledgement_is_deferred_on_idle_partition : KafkaIntegrationTestBase
{
    private string topic = null!;
    private long? committedWhileIdle;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        var consumer = new DeferredAcceptConsumer(TimeSpan.FromMilliseconds(500));

        await CreateAndStartBus(cfg => cfg
            .WithCommitTimeout(TimeSpan.FromSeconds(1))
            .On(this.topic)
            .RequiresAccept()
            .ReactWith(consumer));

        await Produce(this.topic, Enumerable.Range(1, 5).Select(i => new DummyMessage { Id = i }));

        await WaitForCondition(() => consumer.AcceptedCount == 5);
        await WaitForCondition(() => GetCommittedOffset(DefaultGroupId, this.topic) == 5, TimeSpan.FromSeconds(15));

        this.committedWhileIdle = GetCommittedOffset(DefaultGroupId, this.topic);
    }

    [Test]
    public void Then_the_deferred_tail_is_committed_by_timeout()
    {
        this.committedWhileIdle.Should().Be(5);
    }

    private sealed class DeferredAcceptConsumer(TimeSpan delay) : IConsumerOf<DummyMessage>
    {
        private readonly ConcurrentQueue<IConsumingContext<DummyMessage>> accepted = new();

        public int AcceptedCount => this.accepted.Count;

        public Task HandleAsync(IConsumingContext<DummyMessage> context)
        {
            // Как батчевый обработчик: решение принимается позже, на другом потоке.
            _ = Task.Run(async () =>
            {
                await Task.Delay(delay);
                await context.AcceptAsync();
                this.accepted.Enqueue(context);
            });

            return Task.CompletedTask;
        }
    }
}
