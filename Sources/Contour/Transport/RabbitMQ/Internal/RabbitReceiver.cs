using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Helpers;
using Contour.Receiving;
using Contour.Transport.RabbitMQ.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitReceiver(RabbitBus bus, RabbitReceiverOptions receiverOptions, IConnectionPool<IRabbitConnection> connectionPool)
    : AbstractReceiver<RabbitReceiverOptions, RabbitBus, RabbitReceivingSource>(receiverOptions, bus)
{
    private readonly ConcurrentDictionary<Guid, RabbitReceivingSource> receivingSources = new();

    public override IEnumerable<RabbitReceivingSource> ReceivingSources => this.receivingSources.Values;

    protected override void ClearReceivingSources()
    {
        this.receivingSources?.Clear();
    }

    /// <summary>
    /// Builds a set of receiving sources constructing one source for each URL in the connection string
    /// </summary>
    protected override async Task BuildReceivingSources(CancellationToken token)
    {
        var connectionString = new RabbitConnectionString(Configuration.ConnectionString);
        this.Logger.LogTrace("""
                             Building receiving sources of [{Label}]:
                             	{Sources}
                             """,
            Configuration.Label,
            string.Join("\r\n\t", connectionString.Select(url => $"{nameof(RabbitReceivingSource)}({Configuration.Label}): URL\t=>\t{ConnectionStringHelper.Sanitize(url)}")));

        foreach (var url in connectionString)
        {
            CreateReceivingSource(url);
        }

        Configuration.ReceiverRegistration?.Invoke(this);

        foreach (var (_, source) in this.receivingSources)
        {
            token.ThrowIfCancellationRequested();
            await source.Prepare(token);
        }
    }

    private void CreateReceivingSource(string url)
    {
        var newSource = new RabbitReceivingSource(
            Bus,
            url,
            connectionPool,
            Configuration);

        this.receivingSources.TryAdd(newSource.Id, newSource);
        SubscribeToSourceEvents(newSource);
    }
}