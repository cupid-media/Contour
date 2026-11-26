using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.BusLifecycle;

public class When_bus_is_stopped_while_processing_request : RabbitIntegrationTestBase
{
    private IBus bus;
    private string queueName;
    private bool processingFinished;
    private DummyMessage response;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.queueName = GetRandomQueueName();
        this.bus = await CreateBus(DefaultEndpointName, c =>
        {
            c.On(this.queueName)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(async _ =>
                {
                    await Task.Delay(3000);
                    this.processingFinished = true;

                    return new DummyMessage
                    {
                        Id = 1
                    };
                }));

            c.Route(this.queueName)
                .WithDefaultCallbackEndpoint();
        });
        await this.bus.Start();
        var responseTask = this.bus.RequestAsync<DummyMessage, DummyMessage>(this.queueName, new DummyMessage { Id = 0 });
        await Task.Delay(1000);
        await this.bus.Stop();
        this.response = (await responseTask).Response;
    }

    [Test]
    public void Then_bus_should_be_stopped()
    {
        this.bus.State.Should().Be(BusState.Stopped);
    }

    [Test]
    public void Then_request_should_be_processed()
    {
        this.processingFinished.Should().BeTrue();
    }

    [Test]
    public void Then_response_is_returned()
    {
        this.response.Should().NotBeNull();
        this.response.Id.Should().Be(1);
    }

    [Test]
    public async Task Then_queue_exists_without_consumers()
    {
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.queueName))?.Consumers == 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.queueName);
        queue.Should().NotBeNull();
        queue.Consumers.Should().Be(0);
    }
}
