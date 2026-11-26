using System;

namespace Contour.Transport.Kafka.Configuration;

/// <summary>
///   Проверки имён топиков.
/// </summary>
/// <remarks>
///   Имя топика используется как <see cref="MessageLabel"/>, а метки в Contour приводятся
///   к нижнему регистру. Имена топиков в Kafka регистрозависимы, поэтому топик с заглавными
///   буквами уехал бы в свой lower-case вариант — молча и не туда. Такие имена отклоняются
///   при конфигурировании, а не превращаются в потерянные сообщения.
/// </remarks>
internal static class KafkaTopic
{
    public static string Validated(string topic, string paramName)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Label must be specified", paramName);
        }

        if (topic != topic.ToLowerInvariant())
        {
            throw new ArgumentException(
                $"Label [{topic}] must be lower case: Contour normalises message labels to lower case, " +
                "while Kafka topic names are case-sensitive, so a mixed-case name would be published " +
                $"to [{topic.ToLowerInvariant()}] instead.",
                paramName);
        }

        return topic;
    }

    /// <summary>
    ///   Имя топика, производное от имени конечной точки: приводится к нижнему регистру,
    ///   потому что имя конечной точки на регистр не ограничено.
    /// </summary>
    public static string DerivedFrom(string endpointAddress, string suffix)
    {
        return $"{endpointAddress}.{suffix}".ToLowerInvariant();
    }

    /// <summary>
    ///   Fault-топик конечной точки: один на всю конечную точку, как fault-очередь RabbitMQ.
    /// </summary>
    internal static MessageLabel FaultOf(string endpointAddress)
    {
        return MessageLabel.From(DerivedFrom(endpointAddress, "fault"));
    }
}
