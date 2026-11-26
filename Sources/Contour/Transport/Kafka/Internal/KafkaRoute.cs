namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Адрес доставки сообщения в Kafka — имя топика.
/// </summary>
internal sealed record KafkaRoute(string Topic) : IRoute
{
    public override string ToString()
    {
        return Topic;
    }
}
