using System.Collections.Generic;
using Contour.Configuration;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Transport.RabbitMQ.Configuration;

internal record RabbitBusConfiguration : BusConfigurationBase
{
    public IReadOnlyCollection<RabbitReceiverOptions> ReceiverConfigurations { get; init; }

    public IReadOnlyCollection<RabbitSenderOptions> SenderConfigurations { get; init; }

    public IConnectionPool<IRabbitConnection> ConnectionPool { get; init; }
}
