using System;
using Contour.Configuration;

namespace Contour.Transport.RabbitMQ.Configuration;

public class EndpointConfiguration : IRabbitEndpoint
{
    public string Name { get; set; }
    public string ConnectionString { get; set; }
    public bool? ReuseConnection { get; set; }
    public uint? ParallelismLevel { get; set; }
    public TimeSpan? FaultQueueTtl { get; set; }
    public int? FaultQueueLimit { get; set; }
    public int? QueueLimit { get; set; }
    public int? QueueMaxLengthBytes { get; set; }
    public string ConnectionStringProvider { get; set; }
    public string LifecycleHandler { get; set; }
    public string[] ExcludedHeaders { get; set; }
    public IQos Qos { get; set; }
    public IDynamic Dynamic { get; set; }
    public IIncoming[] Incoming { get; set; }
    public IOutgoing[] Outgoing { get; set; }
}
