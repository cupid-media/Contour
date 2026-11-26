using System.Threading.Tasks;

namespace Contour.Receiving;

/// <summary>
/// The FailedDeliveryStrategy interface.
/// </summary>
public interface IFailedDeliveryStrategy
{
    /// <summary>
    /// The handle.
    /// </summary>
    /// <param name="failedConsumingContext">
    /// The failed consuming context.
    /// </param>
    Task Handle(IFailedConsumingContext failedConsumingContext);
}