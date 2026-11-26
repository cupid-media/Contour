
namespace Contour.Transport.RabbitMQ.Configuration;

public class QosConfiguration : IQos
{
    public ushort? PrefetchCount { get; set; }
    public uint? PrefetchSize { get; set; }
}
