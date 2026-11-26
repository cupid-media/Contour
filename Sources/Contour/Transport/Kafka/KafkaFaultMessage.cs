using System;
using System.Text;
using Contour.Transport.Kafka.Internal;

namespace Contour.Transport.Kafka;

/// <summary>
///   Сообщение, перекладываемое в fault-топик: исходная нагрузка плюс сериализуемое
///   описание исключения.
/// </summary>
/// <remarks>
///   Нагрузка сохраняется как UTF-8 строка исходных байтов, а не как десериализованный
///   объект — так же, как это делает RabbitMQ-транспорт. Обработчик мог упасть именно
///   на десериализации, и тогда объекта просто нет.
/// </remarks>
public class KafkaFaultMessage : FaultMessage
{
    /// <summary>
    ///   Для десериализации на стороне потребителя fault-топика.
    /// </summary>
    public KafkaFaultMessage()
    {
    }

    internal KafkaFaultMessage(KafkaDelivery delivery, Exception exception)
    {
        Label = delivery.Topic;
        Payload = delivery.Payload == null ? null : Encoding.UTF8.GetString(delivery.Payload);
        ContentType = delivery.PayloadConverter?.ContentType;
        Received = DateTime.UtcNow;
        Exception = exception != null ? new FaultException(exception) : null;
    }
}
