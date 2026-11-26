using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Emit;

public class When_emitting_message_to_self : RabbitIntegrationTestBase
{
    private bool consumed;

    [OneTimeSetUp]
    public async Task Setup()
    {
        var testQueue = GetRandomQueueName();
        var bus = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(testQueue);

            cfg.On(testQueue)
                .ReactWith(new TestConsumer<DummyMessage>(_ =>
                {
                    this.consumed = true;
                }));
        });

        await bus.EmitAsync(testQueue, new DummyMessage
        {
            Id = 1
        });

        await Task.Delay(1000);
    }

    [Test]
    public void Then_message_is_received()
    {
        this.consumed.Should().BeTrue();
    }
}
