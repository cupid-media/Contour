using System.Threading;
using RabbitMQ.Client.Events;

namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitChannelShutdownEventArgs(RabbitChannel rabbitChannel, ShutdownEventArgs shutdownEventArgs, CancellationToken cancellationToken) : AsyncEventArgs(cancellationToken)
{
    public RabbitChannel RabbitChannel { get; } = rabbitChannel;

    public ShutdownEventArgs ShutdownEventArgs { get; } = shutdownEventArgs;
}
