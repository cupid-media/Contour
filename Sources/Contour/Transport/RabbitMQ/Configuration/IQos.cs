namespace Contour.Transport.RabbitMQ.Configuration;

public interface IQos
{
    ushort? PrefetchCount { get; }

    uint? PrefetchSize { get; }
}