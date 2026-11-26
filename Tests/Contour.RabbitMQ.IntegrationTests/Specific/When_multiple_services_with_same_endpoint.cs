using Contour.RabbitMQ.IntegrationTests.Helpers;
using FluentAssertions;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_multiple_services_with_same_endpoint : RabbitIntegrationTestBase
{
    private const int ServicesCount = 3;
    private string testQueueName;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.testQueueName = GetRandomQueueName();

        for (var i = 0; i < ServicesCount; i++)
        {
            await CreateAndStartBus(DefaultEndpointName, cfg =>
            {
                cfg.On(this.testQueueName)
                    .ReactWith(new TestConsumer<DummyMessage>(_ => {}));
            });
        }
    }

    [Test]
    public async Task Then_queue_contains_expected_number_of_consumers()
    {
        await WaitForCondition(async () => (await GetQueue(DefaultEndpointName, this.testQueueName))?.Consumers > 0, TimeSpan.FromSeconds(10));
        var queue = await GetQueue(DefaultEndpointName, this.testQueueName);
        queue.Consumers.Should().Be(ServicesCount);
    }
}
