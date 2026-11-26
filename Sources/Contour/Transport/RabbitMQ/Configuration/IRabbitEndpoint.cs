using System;

using Contour.Configuration;

namespace Contour.Transport.RabbitMQ.Configuration;

public interface IRabbitEndpoint
{
    string Name { get; }

    IQos Qos { get; }

    IDynamic Dynamic { get; }

    string ConnectionString { get; }

    bool? ReuseConnection { get; }

    uint? ParallelismLevel { get; }

    TimeSpan? FaultQueueTtl { get; }

    int? FaultQueueLimit { get; }

    int? QueueLimit { get; }

    int? QueueMaxLengthBytes { get; }

    string ConnectionStringProvider { get; }

    IIncoming[] Incoming { get; }

    string LifecycleHandler { get; }

    IOutgoing[] Outgoing { get; }

    string[] ExcludedHeaders { get; }
}
