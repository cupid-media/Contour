namespace Contour.Tests.TestConfiguration;

public class TestBusLifecycleHandler : IBusLifecycleHandler
{
    public void OnStarted(IBus bus) { }
    public void OnStopped(IBus bus) { }
    public void OnStarting(IBus bus) { }
    public void OnStopping(IBus bus) { }
}
