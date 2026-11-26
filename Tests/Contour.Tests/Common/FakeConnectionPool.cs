namespace Contour.Tests.Common;

internal class FakeConnectionPool : ConnectionPool<IConnection>
{
    public FakeConnectionPool(IConnectionProvider<IConnection> provider)
    {
        this.Provider = provider;
    }
}