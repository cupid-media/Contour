using System;
using Contour.Transport.RabbitMQ.Configuration;

namespace Contour.Transport.RabbitMQ.Configuration;

public class OutgoingConfiguration : IOutgoing
{
    public string Key { get; set; }
    public string Label { get; set; }
    public bool Confirm { get; set; }
    public bool Persist { get; set; }
    public TimeSpan? Timeout { get; set; }
    public TimeSpan? Ttl { get; set; }
    public string ConnectionString { get; set; }
    public bool? ReuseConnection { get; set; }
    public bool Delayed { get; set; }
    public TimeSpan? ConfirmTimeout { get; set; }
    public ICallbackEndpoint CallbackEndpoint { get; set; }
}
