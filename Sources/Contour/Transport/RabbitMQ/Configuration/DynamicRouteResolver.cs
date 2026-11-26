using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;
using Contour.Helpers;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Topology;

namespace Contour.Transport.RabbitMQ.Configuration;

public class DynamicRouteResolver(RabbitRouteResolverBuilderParams routeResolverBuilderParams)
    : IRouteResolver
{
    private ImmutableHashSet<string> createdExchangesCache = ImmutableHashSet.Create<string>();

    private readonly SemaphoreSlim semaphore = new(1, 1);

    public async ValueTask<IRoute> Resolve(IEndpoint endpoint, MessageLabel label)
    {
        await this.semaphore.ExecuteWithCondition(() => CreateExchange(label), () => !IsExchangeCreated(label.Name));

        return new RabbitRoute(label.Name);
    }

    private async Task<int> CreateExchange(MessageLabel label)
    {
        await routeResolverBuilderParams.Topology.Declare(Exchange.Named(label.Name).Durable.Fanout, CancellationToken.None);
        this.createdExchangesCache = this.createdExchangesCache.Add(label.Name);

        return 0;
    }

    private bool IsExchangeCreated(string exchangeName) => this.createdExchangesCache.Contains(exchangeName);
}
