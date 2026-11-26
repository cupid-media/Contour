using System.Threading.Tasks;

namespace Contour.Receiving.Consumers;

using System;

/// <summary>
/// The lazy consumer of.
/// </summary>
/// <typeparam name="T">
/// </typeparam>
public class LazyConsumerOf<T> : IConsumerOf<T>, IAsyncDisposable
    where T : class
{
    /// <summary>
    /// The _handler.
    /// </summary>
    private readonly Lazy<IConsumer<T>> handler;

    private IConsumerOf<T> consumer;

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="LazyConsumerOf{T}"/>.
    /// </summary>
    /// <param name="handlerResolver">
    /// The handler resolver.
    /// </param>
    public LazyConsumerOf(Func<object> handlerResolver)
    {
        this.handler = new Lazy<IConsumer<T>>(() => (IConsumer<T>)handlerResolver(), true);
    }

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="LazyConsumerOf{T}"/>.
    /// </summary>
    /// <param name="handlerResolver">
    /// The handler resolver.
    /// </param>
    public LazyConsumerOf(Func<IConsumer<T>> handlerResolver)
    {
        this.handler = new Lazy<IConsumer<T>>(handlerResolver, true);
    }

    public Task HandleAsync(IConsumingContext<T> context)
    {
        if (!this.handler.IsValueCreated)
        {
            this.consumer = this.handler.Value as IConsumerOf<T>;
        }

        return this.consumer != null
            ? this.consumer.HandleAsync(context)
            : throw new Exception($"This type: [{this.handler.Value.GetType()}] is not implement IConsumerOf<T>");
    }

    public ValueTask DisposeAsync()
    {
        if (this.consumer is IAsyncDisposable asyncDisposable)
        {
            return asyncDisposable.DisposeAsync();
        }

        return ValueTask.CompletedTask;
    }
}
