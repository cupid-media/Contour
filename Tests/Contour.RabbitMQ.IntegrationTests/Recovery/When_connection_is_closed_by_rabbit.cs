using Contour.RabbitMQ.IntegrationTests.Helpers;
using EasyNetQ.Management.Client;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Recovery;

public class When_connection_is_closed_by_rabbit : RabbitIntegrationTestBase
{
    private const int ParallelismLevel = 5;
    private string testQueue;
    private bool requested;
    private DummyMessage response;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testQueue = GetRandomQueueName();
        var bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(this.testQueue)
                .WithDefaultCallbackEndpoint();

            cfg.On(this.testQueue)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(m =>
                {
                    this.requested = true;
                    return m;
                }))
                .WithParallelismLevel(ParallelismLevel);
        });

        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.testQueue))?.Consumers > 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.testQueue);
        await ManagementClient.CloseConnectionAsync(queue.ConsumerDetails.First().ChannelDetails.ConnectionName);

        await Task.Delay(TimeSpan.FromSeconds(5));
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.testQueue))?.Consumers > 0, TimeSpan.FromMinutes(1));

        this.response = (await bus.RequestAsync<DummyMessage, DummyMessage>(this.testQueue, new DummyMessage
        {
            Id = 100500
        })).Response;
    }

    [Test]
    public void Then_message_is_requested()
    {
        this.requested.Should().BeTrue();
    }

    [Test]
    public void Then_response_is_received()
    {
        this.response.Id.Should().Be(100500);
    }

    [Test]
    public async Task Then_consumers_count_is_equal_to_parallelism_level()
    {
        var queue = await GetQueue(DefaultEndpointName, this.testQueue);
        queue.Consumers.Should().Be(ParallelismLevel);
    }
}
