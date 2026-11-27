using Contour.Helpers;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitConnectionProvider : IConnectionProvider<IRabbitConnection>
{
    private readonly ILogger logger = LogManager.GetLogger<RabbitConnectionProvider>();
    private readonly IEndpoint endpoint;
    private readonly RabbitBus bus;

    public RabbitConnectionProvider(RabbitBus bus)
    {
        this.endpoint = bus.Endpoint;
        this.bus = bus;
    }

    public IRabbitConnection Create(string connectionString)
    {
        this.logger.LogTrace("Creating a new connection for endpoint [{Endpoint}] at [{ConnectionString}]", this.endpoint, ConnectionStringHelper.Sanitize(connectionString));
        return new RabbitConnection(this.endpoint, connectionString, this.bus);
    }
}