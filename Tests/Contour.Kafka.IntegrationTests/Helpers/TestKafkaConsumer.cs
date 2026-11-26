using System.Collections.Concurrent;
using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace Contour.Kafka.IntegrationTests.Helpers;

/// <summary>
/// Consumer that records every handled message and optionally runs a custom handler.
/// </summary>
public class TestKafkaConsumer<TModel> : IConsumerOf<TModel>
    where TModel : class
{
    private readonly Action<Message<TModel>>? handleAction;

    private readonly Func<Message<TModel>, Task>? handleActionAsync;

    public TestKafkaConsumer()
    {
    }

    public TestKafkaConsumer(Action<Message<TModel>> handleAction)
    {
        this.handleAction = handleAction;
    }

    public TestKafkaConsumer(Func<Message<TModel>, Task> handleAction)
    {
        this.handleActionAsync = handleAction;
    }

    public ConcurrentQueue<Message<TModel>> Handled { get; } = new();

    public int HandledCount => this.Handled.Count;

    public async Task HandleAsync(IConsumingContext<TModel> context)
    {
        this.Handled.Enqueue(context.Message);

        if (this.handleAction != null)
        {
            this.handleAction(context.Message);
        }

        if (this.handleActionAsync != null)
        {
            await this.handleActionAsync(context.Message);
        }
    }
}
