using Contour.RabbitMQ.IntegrationTests.Helpers;
using Contour.Sending;
using Contour.Sending.Exceptions;
using FluentAssertions;
using Moq;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_failed_to_send_message : RabbitIntegrationTestBase
{
    private string testQueueName;

    private int consumedMessages = 0;

    private Exception? exception;

    private Mock<IRouteResolver> routeResolverMock;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testQueueName = GetRandomQueueName();

        this.routeResolverMock = new Mock<IRouteResolver>();
        this.routeResolverMock.Setup(r => r.Resolve(It.IsAny<IEndpoint>(), It.IsAny<MessageLabel>()))
            .ThrowsAsync(new Exception("Simulated failure to resolve route"));

        var bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.On(this.testQueueName)
                .ReactWith(new TestConsumer<DummyMessage, DummyMessage>(_ =>
                {
                    Interlocked.Increment(ref this.consumedMessages);
                    return new DummyMessage();
                }));

            cfg.Route(this.testQueueName)
                .WithDefaultCallbackEndpoint()
                .ConfiguredWith((_, _) => new ValueTask<IRouteResolver>(this.routeResolverMock.Object));
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
    public void Then_failover_exception_is_thrown()
    {
        var failoverException = this.exception.Should().BeOfType<FailoverException>().Which;
        failoverException.Attempts.Should().Be(7);
        failoverException.InnerException.Should().BeOfType<AggregateException>()
            .Which.InnerExceptions.Should().HaveCount(7)
            .And.AllBeOfType<Exception>()
                .Which.Should().AllSatisfy(m => m.Message.Should().Be("Simulated failure to resolve route"));
    }

    [Test]
    public void Then_were_7_attempts_to_send()
    {
        this.routeResolverMock.Verify(m => m.Resolve(It.IsAny<IEndpoint>(), It.IsAny<MessageLabel>()), Times.Exactly(7));
    }

    [Test]
    public void Then_consumed_messages_should_be_empty()
    {
        this.consumedMessages.Should().Be(0);
    }
}
