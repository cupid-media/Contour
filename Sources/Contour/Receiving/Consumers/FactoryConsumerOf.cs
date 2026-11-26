using System.Threading.Tasks;

namespace Contour.Receiving.Consumers;

using System;

/// <summary>
/// The factory consumer of.
/// </summary>
/// <typeparam name="T">
/// </typeparam>
public class FactoryConsumerOf<T> : IConsumerOf<T>
    where T : class
{
    /// <summary>
    /// The _handler resolver.
    /// </summary>
    private readonly Func<IConsumer<T>> _handlerResolver;

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="FactoryConsumerOf{T}"/>.
    /// </summary>
    /// <param name="handlerResolver">
    /// The handler resolver.
    /// </param>
    public FactoryConsumerOf(Func<IConsumer<T>> handlerResolver)
    {
        this._handlerResolver = handlerResolver;
    }

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="FactoryConsumerOf{T}"/>.
    /// </summary>
    /// <param name="handlerResolver">
    /// The handler resolver.
    /// </param>
    public FactoryConsumerOf(Func<object> handlerResolver)
    {
        this._handlerResolver = () => (IConsumer<T>)handlerResolver();
    }

    public Task HandleAsync(IConsumingContext<T> context)
    {
        return ((IConsumerOf<T>)this._handlerResolver()).HandleAsync(context);
    }
}