using Contour.RabbitMQ.IntegrationTests.Helpers;
using Contour.Transport.RabbitMQ.Configuration;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_qos_specified : RabbitIntegrationTestBase
{
    private int consumedMessages = 0;
    private string testQueueName;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testQueueName = GetRandomQueueName();

        await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testQueueName)
                .WithQoS(new QoSParams(5, 0))
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
    public void Then_consumed_messages_should_be_1()
    {
        this.consumedMessages.Should().Be(1);
    }

    [Test]
    public async Task Then_only_one_consumer_on_queue()
    {
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.testQueueName))?.Consumers > 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.testQueueName);
        queue.Consumers.Should().Be(1);
    }
}
