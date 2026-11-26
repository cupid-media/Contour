namespace Contour;

/// <summary>
/// The BusLifecycleHandler interface.
/// </summary>
public interface IBusLifecycleHandler
{
    /// <summary>
    /// The on started.
    /// </summary>
    /// <param name="bus">
    /// The bus.
    /// </param>
    void OnStarted(IBus bus);

    /// <summary>
    /// The on starting.
    /// </summary>
    /// <param name="bus">
    /// The bus.
    /// </param>
    void OnStarting(IBus bus);

    /// <summary>
    /// The on stopped.
    /// </summary>
    /// <param name="bus">
    /// The bus.
    /// </param>
    void OnStopped(IBus bus);

    /// <summary>
    /// The on stopping.
    /// </summary>
    /// <param name="bus">
    /// The bus.
    /// </param>
    void OnStopping(IBus bus);
}