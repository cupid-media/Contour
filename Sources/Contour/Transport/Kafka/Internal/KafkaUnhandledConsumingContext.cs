using Contour.Receiving;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Контекст сообщения Kafka, для которого не нашлось обработчика.
/// </summary>
internal sealed class KafkaUnhandledConsumingContext(KafkaDelivery delivery)
    : FaultedConsumingContext(delivery), IUnhandledConsumingContext
{
    public override FaultMessage BuildFaultMessage()
    {
        return new KafkaFaultMessage((KafkaDelivery)Delivery, null);
    }
}
