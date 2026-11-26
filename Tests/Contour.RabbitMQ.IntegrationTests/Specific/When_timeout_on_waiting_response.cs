using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_timeout_on_waiting_response : RabbitIntegrationTestBase
{
    private string testQueueName;

    private int consumedMessages = 0;

    private Exception exception;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testQueueName = GetRandomQueueName();

        var bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testQueueName)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(async _ =>
                {
                    Interlocked.Increment(ref this.consumedMessages);
                    await Task.Delay(TimeSpan.FromSeconds(3));
                    return new DummyMessage();
                }));

            cfg.Route(this.testQueueName)
                .WithRequestTimeout(TimeSpan.FromSeconds(1))
                .WithDefaultCallbackEndpoint();
        });

        try
        {
            await bus.RequestAsync<DummyMessage, DummyMessage>(this.testQueueName, new DummyMessage());
        }
        catch (Exception e)
        {
            this.exception = e;
        }
    }

    [Test]
    public void Then_timeout_exception_is_thrown()
    {
        this.exception.Should().BeOfType<TimeoutException>();
    }

    [Test]
    public void Then_consumed_messages_should_be_1()
    {
        this.consumedMessages.Should().Be(1);
    }
}
