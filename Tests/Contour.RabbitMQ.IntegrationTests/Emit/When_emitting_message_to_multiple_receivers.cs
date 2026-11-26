using System.Collections.Concurrent;
using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Emit;

public class When_emitting_message_to_multiple_receivers : RabbitIntegrationTestBase
{
    private const int ConsumersCount = 5;

    private readonly ConcurrentDictionary<int, bool> consumed = new();

    [OneTimeSetUp]
    public async Task Setup()
    {
        var testQueue = GetRandomQueueName();
        for (var i = 0; i < ConsumersCount; i++)
        {
            var index = i;
            await CreateAndStartBus($"{DefaultEndpointName}.{index}", cfg =>
            {
                cfg.On(testQueue)
                    .ReactWith(new TestConsumer<DummyMessage>(_ =>
                    {
                        this.consumed.TryAdd(index, true);
                    }));
            });
        }

        var publish = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.Route(testQueue);
        });

        await publish.EmitAsync(testQueue, new DummyMessage
        {
            Id = 1
        });

        await Task.Delay(1000);
    }

    [Test]
    public void Then_message_is_received_in_all_receivers()
    {
        this.consumed.Should().HaveCount(ConsumersCount);
        this.consumed.Should().AllSatisfy(c => c.Value.Should().BeTrue());
    }
}
