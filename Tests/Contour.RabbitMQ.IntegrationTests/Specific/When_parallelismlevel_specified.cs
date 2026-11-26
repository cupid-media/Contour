using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_parallelismlevel_specified : RabbitIntegrationTestBase
{
    private const int ParallelismLevel = 5;

    private int consumedMessages = 0;
    private string testQueueName;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testQueueName = GetRandomQueueName();

        await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testQueueName)
                .WithParallelismLevel(ParallelismLevel)
                .ReactWith(new TestConsumer<DummyMessage>(async _ =>
                {
                    Interlocked.Increment(ref this.consumedMessages);
                    await Task.Delay(3000);
                }));
        });

        var publisherBus = await CreateAndStartBus(DefaultEndpointName + ".Publish", cfg =>
        {
            cfg.Route(this.testQueueName);
        });

        for (var i = 0; i < 10; i++)
        {
            await publisherBus.EmitAsync(this.testQueueName, new DummyMessage());
        }

        await Task.Delay(1000);
    }

    [Test]
    public void Then_consumed_messages_should_be_5()
    {
        this.consumedMessages.Should().Be(ParallelismLevel);
    }

    [Test]
    public async Task Then_five_consumer_on_queue_with_different_channels()
    {
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.testQueueName))?.Consumers > 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.testQueueName);
        queue.Consumers.Should().Be(ParallelismLevel);
        queue.ConsumerDetails.Select(d => d.ChannelDetails.Number).Distinct().Should().HaveCount(ParallelismLevel);
    }
}
