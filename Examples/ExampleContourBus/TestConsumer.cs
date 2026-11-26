using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace ExampleContourBus;

public class TestConsumer : IConsumerOf<TestModel>
{
    public Task HandleAsync(IConsumingContext<TestModel> context)
    {
        Console.WriteLine("Message received");
        return Task.CompletedTask;
    }
}
