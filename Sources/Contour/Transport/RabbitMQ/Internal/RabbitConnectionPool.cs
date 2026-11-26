namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitConnectionPool : ConnectionPool<IRabbitConnection>
{
    public RabbitConnectionPool(RabbitBus bus)
    {
        Provider = new RabbitConnectionProvider(bus);
    }
}