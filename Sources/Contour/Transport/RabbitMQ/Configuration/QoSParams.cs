namespace Contour.Transport.RabbitMQ.Configuration;

/// <summary>
///   QoS настройки для RabbitMQ.
/// </summary>
public record QoSParams(ushort PrefetchCount, uint PrefetchSize);
