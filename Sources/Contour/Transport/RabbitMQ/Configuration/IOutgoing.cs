using System;

namespace Contour.Transport.RabbitMQ.Configuration;

public interface IOutgoing
{
    string Key { get; }

    string Label { get; }

    ICallbackEndpoint CallbackEndpoint { get; }

    bool Confirm { get; }

    bool Persist { get; }

    TimeSpan? Timeout { get; }

    TimeSpan? Ttl { get; }

    string ConnectionString { get; }

    bool? ReuseConnection { get; }

    bool Delayed { get; }

    TimeSpan? ConfirmTimeout { get; }
}
