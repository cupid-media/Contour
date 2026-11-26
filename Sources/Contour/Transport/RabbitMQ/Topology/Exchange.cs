using System.Collections.Generic;
using System.Threading.Tasks;
using Contour.Sending;
using Contour.Topology;
using RabbitMQ.Client;

namespace Contour.Transport.RabbitMQ.Topology;

/// <summary>
/// The exchange.
/// </summary>
public class Exchange : ITopologyEntity, IRouteResolver
{
    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="Exchange"/>.
    /// </summary>
    /// <param name="name">
    /// The name.
    /// </param>
    internal Exchange(string name)
    {
        Name = name;
        Type = ExchangeType.Direct;
        Durable = false;
        AutoDelete = false;
        Arguments = new Dictionary<string, object>();
    }

    /// <summary>
    /// Gets a value indicating whether auto delete.
    /// </summary>
    public bool AutoDelete { get; internal set; }

    /// <summary>
    /// Gets a value indicating whether durable.
    /// </summary>
    public bool Durable { get; internal set; }

    /// <summary>
    /// Gets the name.
    /// </summary>
    public string Name { get; internal set; }

    /// <summary>
    /// Gets the type.
    /// </summary>
    public string Type { get; internal set; }

    public IDictionary<string, object> Arguments { get; }

    /// <summary>
    /// The named.
    /// </summary>
    /// <param name="name">
    /// The name.
    /// </param>
    /// <returns>
    /// The <see cref="ExchangeBuilder"/>.
    /// </returns>
    public static ExchangeBuilder Named(string name)
    {
        return new ExchangeBuilder(name);
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
        return new ValueTask<IRoute>(new RabbitRoute(Name));
    }
}