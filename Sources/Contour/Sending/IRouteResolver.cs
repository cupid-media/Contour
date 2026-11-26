using System.Threading.Tasks;

namespace Contour.Sending;

/// <summary>
/// The RouteResolver interface.
/// </summary>
public interface IRouteResolver
{
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
    ValueTask<IRoute> Resolve(IEndpoint endpoint, MessageLabel label);
}