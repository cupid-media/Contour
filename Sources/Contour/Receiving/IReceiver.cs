using Contour.Receiving.Consumers;

namespace Contour.Receiving;

/// <summary>
/// The Receiver interface.
/// </summary>
public interface IReceiver : IBusComponent
{
    void RegisterConsumer<T>(IConsumer<T> consumer) where T : class;

    int ActiveRequestsCount { get; }
}