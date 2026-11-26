using System.Collections.Concurrent;
using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace Contour.Kafka.IntegrationTests.Helpers;

/// <summary>
/// Responder that records every handled request and replies with a computed response.
/// </summary>
/// <remarks>
/// A responder is an ordinary consumer: it replies through the consuming context, exactly as
/// a RabbitMQ one does. A <c>null</c> response means nothing is sent back.
/// </remarks>
public class TestKafkaResponder<TRequest, TResponse> : IConsumerOf<TRequest>
    where TRequest : class
    where TResponse : class
{
    private readonly Func<Message<TRequest>, TResponse>? handler;

    private readonly Func<Message<TRequest>, Task<TResponse>>? handlerAsync;

    public TestKafkaResponder(Func<Message<TRequest>, TResponse> handler)
    {
        this.handler = handler;
    }

    public TestKafkaResponder(Func<Message<TRequest>, Task<TResponse>> handler)
    {
        this.handlerAsync = handler;
    }

    public ConcurrentQueue<Message<TRequest>> Handled { get; } = new();

    public async Task HandleAsync(IConsumingContext<TRequest> context)
    {
        this.Handled.Enqueue(context.Message);

        var response = this.handlerAsync != null
            ? await this.handlerAsync(context.Message)
            : this.handler!(context.Message);

        if (response != null)
        {
            await context.ReplyAsync(response);
        }
    }
}
