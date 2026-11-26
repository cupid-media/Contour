using System;
using Contour.Receiving;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Контекст сообщения Kafka, обработка которого завершилась исключением.
/// </summary>
internal sealed class KafkaFailedConsumingContext : FaultedConsumingContext, IFailedConsumingContext
{
    public KafkaFailedConsumingContext(KafkaDelivery delivery, Exception exception)
        : base(delivery)
    {
        Exception = exception;
    }

    public Exception Exception { get; }

    public override FaultMessage BuildFaultMessage()
    {
        return new KafkaFaultMessage((KafkaDelivery)Delivery, Exception);
    }
}
