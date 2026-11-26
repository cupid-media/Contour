using Contour.Transport.RabbitMQ.Topology;

namespace Contour.Transport.RabbitMQ.Configuration;

public class RabbitRouteResolverBuilderParams
{
    public IEndpoint Endpoint { get; init; }

    public RabbitSenderOptions Options { get; init; }

    public RabbitTopologyBuilder Topology { get; init; }
}