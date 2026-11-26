using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_response_processed_sync : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;
    private string testRequestQueueName;

    private IBus testBus;
    private IBus helperBus;

    private int receivedCount;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testIncomingQueueName = GetRandomQueueName();
        this.testRequestQueueName = GetRandomQueueName();

        this.testBus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testIncomingQueueName)
                .ReactWith(new TestExtendedConsumer<DummyMessage>(async context =>
                {
                    await this.testBus.RequestAsync<DummyMessage, DummyMessage>(this.testRequestQueueName, new DummyMessage());
                    Interlocked.Increment(ref this.receivedCount);
                    Thread.Sleep(1000); // simulate heavy sync operation
                    await context.AcceptAsync();
                }))
                .WithParallelismLevel(5)
                .RequiresAccept();

            cfg.Route(this.testRequestQueueName)
                .WithDefaultCallbackEndpoint();
        });

        this.helperBus = await CreateAndStartBus(DefaultEndpointName + "2", cfg =>
        {
            cfg.Route(this.testIncomingQueueName);

            cfg.On(this.testRequestQueueName)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(_ => new DummyMessage()))
                .RequiresAccept();
        });

        for (var i = 0; i < 5; i++)
        {
            await this.helperBus.EmitAsync(this.testIncomingQueueName, new DummyMessage());
        }

        await Task.Delay(100);
    }

    [Test]
    public void Then_all_messages_received()
    {
        this.receivedCount.Should().Be(5);
    }
}
