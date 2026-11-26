namespace Contour;

/// <summary>
///   Известные значения <see cref="IBus.Transport"/>.
/// </summary>
public static class BusTransports
{
    /// <summary>
    ///   Шина поверх RabbitMQ.
    /// </summary>
    public const string Rmq = "rmq";

    /// <summary>
    ///   Шина поверх Kafka.
    /// </summary>
    public const string Kafka = "kafka";
}
