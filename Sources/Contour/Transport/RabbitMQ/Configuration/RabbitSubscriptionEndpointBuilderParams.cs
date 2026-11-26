using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Topology;

namespace Contour.Transport.RabbitMQ.Configuration;

public class RabbitSubscriptionEndpointBuilderParams
{
    public IEndpoint Endpoint { get; init; }

    public RabbitReceiverOptions ReceiverOptions { get; init; }

    public RabbitTopologyBuilder Topology { get; set; }

    public Task<ISubscriptionEndpoint> UseDefaultTempReplyEndpoint(ISenderConfiguration senderConfiguration, CancellationToken cancellationToken)
    {
        return Topology.BuildTempReplyEndpoint(Endpoint, senderConfiguration.Label, cancellationToken);
    }
}
