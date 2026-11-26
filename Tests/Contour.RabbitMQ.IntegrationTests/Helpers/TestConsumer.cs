using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace Contour.RabbitMQ.IntegrationTests.Helpers;

public class TestConsumer<TModel> : IConsumerOf<TModel> where TModel : class
{
    private readonly Action<TModel>? handleAction;

    private readonly Func<TModel, Task>? handleActionAsync;

    public TestConsumer(Action<TModel> handleAction)
    {
        this.handleAction = handleAction;
    }

    public TestConsumer(Func<TModel, Task> handleAction)
    {
        this.handleActionAsync = handleAction;
    }

    public async Task HandleAsync(IConsumingContext<TModel> context)
    {
        if (this.handleAction != null)
        {
            this.handleAction(context.Message.Payload);
        }

        if (this.handleActionAsync != null)
        {
            await this.handleActionAsync(context.Message.Payload);
        }

        await context.AcceptAsync();
    }
}

public class TestConsumer<TModel, TResponse> : IConsumerOf<TModel> where TModel : class where TResponse : class
{
    private readonly Func<TModel, TResponse>? handleAction;

    private readonly Func<TModel, Task<TResponse>>? handleActionAsync;

    public TestConsumer(Func<TModel, TResponse> handleAction)
    {
        this.handleAction = handleAction;
    }

    public TestConsumer(Func<TModel, Task<TResponse>> handleAction)
    {
        this.handleActionAsync = handleAction;
    }

    public async Task HandleAsync(IConsumingContext<TModel> context)
    {
        TResponse response = null;
        if (this.handleAction != null)
        {
            response = this.handleAction(context.Message.Payload);
        }

        if (this.handleActionAsync != null)
        {
            response = await this.handleActionAsync(context.Message.Payload);
        }

        await context.ReplyAsync(response);
        await context.AcceptAsync();
    }
}
