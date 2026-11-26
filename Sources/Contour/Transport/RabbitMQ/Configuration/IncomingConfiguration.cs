using Contour.Configuration;
using Contour.Transport.RabbitMQ.Configuration;

namespace Contour.Transport.RabbitMQ.Configuration;

public class IncomingConfiguration : IIncoming
{
    public string Key { get; set; }
    public string Label { get; set; }
    public string Type { get; set; }
    public string React { get; set; }
    public bool RequiresAccept { get; set; }
    public Lifestyle? Lifestyle { get; set; }
    public string ConnectionString { get; set; }
    public bool? ReuseConnection { get; set; }
    public int? QueueLimit { get; set; }
    public int? QueueMaxLengthBytes { get; set; }
    public uint? ParallelismLevel { get; set; }
    public bool Delayed { get; set; }
    public IQos Qos { get; set; }
}
