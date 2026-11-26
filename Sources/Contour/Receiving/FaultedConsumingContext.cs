using System.Threading.Tasks;
using Contour.Helpers;

namespace Contour.Receiving;

/// <summary>
/// The faulted consuming context.
/// </summary>
internal abstract class FaultedConsumingContext : IFaultedConsumingContext
{
    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="FaultedConsumingContext"/>.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    protected FaultedConsumingContext(IDelivery delivery)
    {
        Delivery = delivery;
    }

    /// <summary>
    /// Gets the delivery.
    /// </summary>
    public IDelivery Delivery { get; private set; }

    /// <summary>
    /// The accept.
    /// </summary>
    public Task AcceptAsync()
    {
        return Delivery.AcceptAsync();
    }

    /// <summary>
    /// The build fault message.
    /// </summary>
    /// <returns>
    /// The <see cref="FaultMessage"/>.
    /// </returns>
    public abstract FaultMessage BuildFaultMessage();

    /// <summary>
    /// The forward.
    /// </summary>
    /// <param name="label">
    /// The label.
    /// </param>
    /// <param name="message">
    /// The message.
    /// </param>
    /// <typeparam name="T">
    /// </typeparam>
    public Task ForwardAsync<T>(string label, T message) where T : class
    {
        return Delivery.ForwardAsync(label.ToMessageLabel(), message);
    }

    /// <summary>
    /// The reject.
    /// </summary>
    /// <param name="requeue">
    /// The requeue.
    /// </param>
    public Task RejectAsync(bool requeue)
    {
        return Delivery.RejectAsync(requeue);
    }
}