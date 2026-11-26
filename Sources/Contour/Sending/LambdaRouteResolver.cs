using System.Threading.Tasks;

namespace Contour.Sending;

using System;

/// <summary>
/// The lambda route resolver.
/// </summary>
public class LambdaRouteResolver : IRouteResolver
{
    /// <summary>
    /// The _resolver func.
    /// </summary>
    private readonly Func<IEndpoint, MessageLabel, IRoute> resolverFunc;

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="LambdaRouteResolver"/>.
    /// </summary>
    /// <param name="resolverFunc">
    /// The resolver func.
    /// </param>
    public LambdaRouteResolver(Func<IEndpoint, MessageLabel, IRoute> resolverFunc)
    {
        this.resolverFunc = resolverFunc;
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
        return new ValueTask<IRoute>(this.resolverFunc(endpoint, label));
    }
}
