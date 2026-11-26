using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace Contour.RabbitMQ.IntegrationTests.Helpers;

public class TestExtendedConsumer<TModel> : IConsumerOf<TModel> where TModel : class
{
    private readonly Action<IConsumingContext<TModel>>? handleAction;

    private readonly Func<IConsumingContext<TModel>, Task>? handleActionAsync;

    public TestExtendedConsumer(Action<IConsumingContext<TModel>> handleAction)
    {
        this.handleAction = handleAction;
    }

    public TestExtendedConsumer(Func<IConsumingContext<TModel>, Task> handleAction)
    {
        this.handleActionAsync = handleAction;
    }

    public async Task HandleAsync(IConsumingContext<TModel> context)
    {
        if (this.handleAction != null)
        {
            this.handleAction(context);
        }

        if (this.handleActionAsync != null)
        {
            await this.handleActionAsync(context);
        }
    }
}
