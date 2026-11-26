using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Contour.Receiving;
using Contour.Sending;

namespace Contour;

internal class BusComponentTracker : IBusComponentTracker
{
    private readonly ConcurrentBag<IBusComponent> components = [];

    /// <summary>
    /// The all of.
    /// </summary>
    /// <typeparam name="T">
    /// The type of component to search for
    /// </typeparam>
    /// <returns>
    /// The <see cref="IEnumerable{T}"/>.
    /// </returns>
    public IEnumerable<T> AllOf<T>() where T : class, IBusComponent
    {
        return this.components.OfType<T>();
    }

    public void Register(IBusComponent component)
    {
        this.components.Add(component);
    }

    public Task PrepareAll(CancellationToken cancellationToken)
    {
        return Task.WhenAll(this.components.Select(c => c.Prepare(cancellationToken)));
    }

    public Task StartAllSenders(CancellationToken cancellationToken)
    {
        return Task.WhenAll(this.components.OfType<ISender>().Select(c => c.Start(cancellationToken)));
    }

    public Task StopAllSenders(CancellationToken cancellationToken)
    {
        return Task.WhenAll(this.components.OfType<ISender>().Select(c => c.Stop(cancellationToken)));
    }

    public Task StartAllReceivers(CancellationToken cancellationToken)
    {
        return Task.WhenAll(this.components.OfType<IReceiver>().Select(c => c.Start(cancellationToken)));
    }

    public Task StopAllReceivers(CancellationToken cancellationToken)
    {
        return Task.WhenAll(this.components.OfType<IReceiver>().Select(c => c.Stop(cancellationToken)));
    }

    public bool CheckHealth()
    {
        return this.components.All(c => c.IsHealthy);
    }

    public async Task UnregisterAll()
    {
        await Task.WhenAll(this.components.Select(c => c.DisposeAsync().AsTask()));
        this.components.Clear();
    }
}