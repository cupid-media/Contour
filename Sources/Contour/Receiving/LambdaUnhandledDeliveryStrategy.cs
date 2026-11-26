using System.Threading.Tasks;

namespace Contour.Receiving;

using System;

using Microsoft.Extensions.Logging;

/// <summary>
/// The lambda unhandled delivery strategy.
/// </summary>
public class LambdaUnhandledDeliveryStrategy : IUnhandledDeliveryStrategy
{
    private static readonly ILogger Log = LogManager.GetLogger<LambdaUnhandledDeliveryStrategy>();

    /// <summary>
    /// The _handler action.
    /// </summary>
    private readonly Func<IUnhandledConsumingContext, Task> _handlerAction;

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="LambdaUnhandledDeliveryStrategy"/>.
    /// </summary>
    /// <param name="handlerAction">
    /// The handler action.
    /// </param>
    public LambdaUnhandledDeliveryStrategy(Func<IUnhandledConsumingContext, Task> handlerAction)
    {
        this._handlerAction = handlerAction;
    }

    /// <summary>
    /// The handle.
    /// </summary>
    /// <param name="unhandledConsumingContext">
    /// The unhandled consuming context.
    /// </param>
    public async Task Handle(IUnhandledConsumingContext unhandledConsumingContext)
    {
        try
        {
            await this._handlerAction(unhandledConsumingContext);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Unable to handle failed message [{Label}]", unhandledConsumingContext.Delivery.Label);
        }
    }
}