namespace Contour.Transport.RabbitMQ;

/// <summary>
///   Описание маршрута для доставки сообщений через RabbitMQ.
/// </summary>
public class RabbitRoute : IRoute
{
    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="RabbitRoute"/>.
    /// Создает описание машрута для доставки сообщений через RabbitMQ.
    /// </summary>
    /// <param name="exchange">
    /// Exchange.
    /// </param>
    /// <param name="routingKey">
    /// Routing key.
    /// </param>
    public RabbitRoute(string exchange, string routingKey = "")
    {
        Exchange = exchange;
        RoutingKey = routingKey;
    }

    /// <summary>
    ///   Имя exchange.
    /// </summary>
    public string Exchange { get; private set; }

    /// <summary>
    ///   Routing key.
    /// </summary>
    public string RoutingKey { get; private set; }

    /// <summary>
    /// The to string.
    /// </summary>
    /// <returns>
    /// The <see cref="string"/>.
    /// </returns>
    public override string ToString()
    {
        return $"Exchange: {Exchange}, RoutingKey: {RoutingKey}";
    }
}