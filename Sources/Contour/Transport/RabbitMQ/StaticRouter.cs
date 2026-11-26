using System.Threading.Tasks;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Topology;

namespace Contour.Transport.RabbitMQ;

/// <summary>
/// The static route resolver.
/// </summary>
public class StaticRouteResolver : IRouteResolver
{
    /// <summary>
    /// The _route.
    /// </summary>
    private readonly IRoute route;

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="StaticRouteResolver"/>.
    /// </summary>
    /// <param name="route">
    /// The route.
    /// </param>
    public StaticRouteResolver(IRoute route)
    {
        this.route = route;
    }

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="StaticRouteResolver"/>.
    /// </summary>
    /// <param name="exchange">
    /// The exchange.
    /// </param>
    /// <param name="routingKey">
    /// The routing key.
    /// </param>
    public StaticRouteResolver(string exchange, string routingKey = "")
    {
        this.route = new RabbitRoute(exchange, routingKey);
    }

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="StaticRouteResolver"/>.
    /// </summary>
    /// <param name="exchange">
    /// The exchange.
    /// </param>
    /// <param name="routingKey">
    /// The routing key.
    /// </param>
    public StaticRouteResolver(Exchange exchange, string routingKey = "")
        : this(exchange.Name, routingKey)
    {
    }

    /// <summary>
    /// The resolve.
    /// </summary>
    /// <param name="endpoint">
    /// The endpoint.
    /// </param>
    /// <param name="label">
    /// The label.
    /// </param>
    /// <returns>
    /// The <see cref="IRoute"/>.
    /// </returns>
    public ValueTask<IRoute> Resolve(IEndpoint endpoint, MessageLabel label)
    {
        return new ValueTask<IRoute>(this.route);
    }
}