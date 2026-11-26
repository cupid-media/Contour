using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace Contour.Tests.TestConfiguration;

public class TestIncomingMessageConsumer : IConsumerOf<TestIncomingMessage>
{
    public Task HandleAsync(IConsumingContext<TestIncomingMessage> context)
    {
        // Test consumer logic
        return Task.CompletedTask;
    }
}
