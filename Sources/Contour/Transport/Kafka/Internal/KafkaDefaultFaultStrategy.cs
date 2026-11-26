using System.Threading.Tasks;
using Contour.Receiving;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Стратегия провалов по умолчанию: перекладывает упавшее сообщение в fault-топик конечной точки.
/// </summary>
/// <remarks>
///   Как и fault-очередь RabbitMQ, fault-топик один на конечную точку и пишется через свой маршрут
///   с настройками отправки уровня шины, а не в кластер, из которого прочитано сообщение.
///   Маршрут объявляет <c>KafkaBusConfigurator.PostConfigure</c>.
///   Ошибка отправки пробрасывается: проглоченная, она означала бы потерю сообщения, а проброшенная
///   удерживает оффсет.
/// </remarks>
internal sealed class KafkaDefaultFaultStrategy : IFailedDeliveryStrategy
{
    public Task Handle(IFailedConsumingContext failedConsumingContext)
    {
        var delivery = (KafkaDelivery)failedConsumingContext.Delivery;

        return delivery.ForwardToFaultAsync(failedConsumingContext.BuildFaultMessage());
    }
}
