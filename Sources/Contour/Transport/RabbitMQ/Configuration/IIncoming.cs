using Contour.Configuration;

namespace Contour.Transport.RabbitMQ.Configuration;

public interface IIncoming
{
    string Key { get; }

    string Label { get; }

    Lifestyle? Lifestyle { get; }

    string React { get; }

    bool RequiresAccept { get; }

    string Type { get; }

    IQos Qos { get; }

    uint? ParallelismLevel { get; }

    string ConnectionString { get; }

    bool? ReuseConnection { get; }

    int? QueueLimit { get; }

    int? QueueMaxLengthBytes { get; }

    bool Delayed { get; }
}
