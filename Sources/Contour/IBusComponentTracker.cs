using System.Threading.Tasks;
using System.Collections.Generic;
using System.Threading;

namespace Contour;

/// <summary>
///   Трекер компонентов клиента шины. Позволяет работать с компонентами пакетно.
/// </summary>
public interface IBusComponentTracker
{
    IEnumerable<T> AllOf<T>() where T : class, IBusComponent;

    void Register(IBusComponent component);

    Task PrepareAll(CancellationToken cancellationToken);

    Task StartAllSenders(CancellationToken cancellationToken);

    Task StopAllSenders(CancellationToken cancellationToken);

    Task StartAllReceivers(CancellationToken cancellationToken);

    Task StopAllReceivers(CancellationToken cancellationToken);

    bool CheckHealth();

    Task UnregisterAll();
}