using System.Threading;
using Contour.Transport.RabbitMQ.Internal;
using RabbitMQ.Client.Events;

namespace Contour.Receiving;

internal class ReceivingSourceStoppedEventArgs : AsyncEventArgs
{
    public ReceivingSourceStoppedEventArgs(IReceivingSource receivingSource, OperationStopReason reason, CancellationToken token) : base(token)
    {
        ReceivingSource = receivingSource;
        Reason = reason;
    }

    public IReceivingSource ReceivingSource { get; }

    public OperationStopReason Reason { get; }
}
