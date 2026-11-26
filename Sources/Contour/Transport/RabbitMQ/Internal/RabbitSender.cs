using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.RabbitMQ.Internal;

/// <summary>
/// Отправитель сообщений с помощью брокера <c>RabbitMQ</c>.
/// </summary>
internal class RabbitSender : AbstractSender<RabbitSenderOptions, RabbitBus, RabbitProducer>
{
    private readonly IConnectionPool<IRabbitConnection> connectionPool;
    private readonly ConcurrentDictionary<Guid, RabbitProducer> producers = new();
    private IFaultTolerantProducer faultTolerantProducer;

    protected override ICollection<RabbitProducer> Producers => this.producers.Values;

    public RabbitSender(RabbitBus bus, RabbitSenderOptions configuration, IConnectionPool<IRabbitConnection> connectionPool)
        : base(bus, configuration)
    {
        this.connectionPool = connectionPool;
    }

    protected override async Task StartProducers(CancellationToken cancellationToken)
    {
        await Task.WhenAll(Producers.Select(x => x.Start(cancellationToken)));
    }

    protected override async Task StopProducers(CancellationToken token)
    {
        this.Logger.LogTrace("Stopping producers for sender of [{Label}]", Configuration.Label);

        foreach (var producer in Producers)
        {
            token.ThrowIfCancellationRequested();
            try
            {
                await producer.Stop(token);
                this.Logger.LogTrace("Producer stopped successfully");
            }
            catch (Exception ex)
            {
                this.Logger.LogError(ex, "Failed to stop producer [{ProducerId}] in sender of [{Label}] due to {Message}", producer.Id, Configuration.Label, ex.Message);
            }
        }
    }

    protected override void ClearProducers()
    {
        this.producers?.Clear();
        this.faultTolerantProducer = null;
    }

    protected override Task<EmitResult> InternalSend<TRequest>(Message<TRequest> message, PublishingOptions options)
    {
        return this.faultTolerantProducer.Send(message, options);
    }

    protected override Task<RequestResult<TResponse>> InternalRequest<TRequest, TResponse>(Message<TRequest> message, RequestOptions options)
    {
        return this.faultTolerantProducer.Request<TRequest, TResponse>(message, options);
    }

    protected override IEnumerable<string> GetUnsupportedOptions(PublishingOptions options)
    {
        foreach (var option in base.GetUnsupportedOptions(options))
        {
            yield return option;
        }

        if (options.Delay != null && !Configuration.Delayed)
        {
            yield return nameof(PublishingOptions.Delay);
        }

        if (options.Key != null)
        {
            yield return nameof(PublishingOptions.Key);
        }
    }

    /// <summary>
    /// Builds a set of producers constructing one producer for each URL in the connection string
    /// </summary>
    protected override async Task PrepareProducers(CancellationToken token)
    {
        var connectionsString = new RabbitConnectionString(Configuration.ConnectionString);
        this.Logger.LogTrace("Building producers of [{Label}]:\r\n\t{Producers}", Configuration.Label, string.Join("\r\n\t", connectionsString.Select(url => $"Producer({Configuration.Label}): URL\t=>\t{url}")));

        foreach (var url in connectionsString)
        {
            token.ThrowIfCancellationRequested();
            var producer = BuildProducer(url);
            await producer.Prepare(token);
        }

        var builder = Configuration.ProducerSelectorBuilder;
        var producerSelector = builder.Build(this.producers.Select(p => p.Value));
        this.faultTolerantProducer = new FaultTolerantProducer(producerSelector, Configuration.FailoverAttempts);
    }

    private RabbitProducer BuildProducer(string url)
    {
        this.Logger.LogTrace("Enlisting a new producer of [{Label}] at URL=[{Url}]...", Configuration.Label, url);
        var producer = new RabbitProducer(Bus, url, this.connectionPool, Configuration);

        if (Configuration.RequiresCallback)
        {
            producer.WithCallback();
            this.Logger.LogTrace("A producer of [{Label}] at URL=[{BrokerUrl}] has registered a callback listener successfully", producer.Label, producer.BrokerUrl);
        }

        this.producers.TryAdd(producer.Id, producer);
        this.Logger.LogTrace("A producer of [{Label}] at URL=[{BrokerUrl}] has been enlisted", producer.Label, producer.BrokerUrl);

        return producer;
    }
}
