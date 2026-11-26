using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal class KafkaReceiver(KafkaBus bus, KafkaReceiverOptions receiverOptions, IKafkaConsumerFactory consumerFactory)
    : AbstractReceiver<KafkaReceiverOptions, KafkaBus, KafkaReceivingSource>(receiverOptions, bus)
{
    private readonly ConcurrentDictionary<Guid, KafkaReceivingSource> receivingSources = new();

    public override IEnumerable<KafkaReceivingSource> ReceivingSources => this.receivingSources.Values;

    public override bool IsHealthy => base.IsHealthy && ReceivingSources.All(s => s.IsHealthy);

    protected override void ClearReceivingSources()
    {
        this.receivingSources?.Clear();
    }

    protected override async Task BuildReceivingSources(CancellationToken token)
    {
        this.Logger.LogTrace("""
                             Building receiving sources of [{Label}]:
                             	{Sources}
                             """,
            Configuration.Label,
            $"{nameof(KafkaReceivingSource)}({Configuration.Label}): URL\t=>\t{Configuration.ConnectionString}");

        CreateReceivingSource(Configuration.ConnectionString);

        Configuration.ReceiverRegistration?.Invoke(this);

        foreach (var (_, source) in this.receivingSources)
        {
            token.ThrowIfCancellationRequested();
            await source.Prepare(token);
        }
    }

    private void CreateReceivingSource(string url)
    {
        var newSource = new KafkaReceivingSource(
            Bus,
            url,
            consumerFactory,
            Configuration);

        this.receivingSources.TryAdd(newSource.Id, newSource);
        SubscribeToSourceEvents(newSource);
    }
}
