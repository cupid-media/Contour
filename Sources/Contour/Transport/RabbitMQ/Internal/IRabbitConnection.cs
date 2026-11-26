using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Contour.Transport.RabbitMQ.Internal;

internal interface IRabbitConnection : IConnection
{
    bool IsBlocked { get; }

    bool IsOpen { get; }

    Task<RabbitChannel> OpenChannel(CreateChannelOptions options = null, CancellationToken token = default);

    /// <summary>
    /// Opens a shared channel which can be used by multiple listeners\publishers. For locks among them use channel.SharedLockObject
    /// Connection supports multiple shared channels which are grouped by key
    /// </summary>
    /// <param name="key">Key of shared channel</param>
    Task<RabbitChannel> OpenSharedChannel(string key, CreateChannelOptions options = null, CancellationToken cancellationToken = default);
}