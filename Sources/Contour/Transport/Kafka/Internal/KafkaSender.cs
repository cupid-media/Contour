using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Sending;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Отправитель сообщений с помощью брокера <c>Kafka</c>.
/// </summary>
internal class KafkaSender(KafkaBus bus, KafkaSenderOptions configuration, IKafkaProducerFactory producerFactory)
    : AbstractSender<KafkaSenderOptions, KafkaBus, KafkaProducer>(bus, configuration)
{
    private readonly ConcurrentDictionary<Guid, KafkaProducer> producers = new();
    private IFaultTolerantProducer faultTolerantProducer;

    protected override ICollection<KafkaProducer> Producers => this.producers.Values;

    public override bool IsHealthy => base.IsHealthy && Producers.All(p => p.IsInGoodCondition);

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
                this.Logger.LogTrace("Producer [{ProducerId}] stopped successfully", producer.Id);
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

        if (options.Delay != null)
        {
            yield return nameof(PublishingOptions.Delay);
        }

        if (options.Ttl != null)
        {
            yield return nameof(PublishingOptions.Ttl);
        }

        if (options.Persistently == true)
        {
            yield return nameof(PublishingOptions.Persistently);
        }
    }

    /// <summary>
    ///   Builds a producer for the cluster in the connection string.
    /// </summary>
    /// <remarks>
    ///   Список брокеров librdkafka разбирает сам, поэтому продюсер на маршрут один,
    ///   а повторы отправки и пересборку сломанного продюсера обеспечивает <see cref="KafkaFaultTolerantProducer"/>.
    /// </remarks>
    protected override async Task PrepareProducers(CancellationToken token)
    {
        var url = Configuration.ConnectionString;
        this.Logger.LogTrace("Building producers of [{Label}]:\r\n\t{Producers}", Configuration.Label, $"Producer({Configuration.Label}): URL\t=>\t{url}");

        token.ThrowIfCancellationRequested();
        var producer = BuildProducer(url);
        await producer.Prepare(token);

        var builder = Configuration.ProducerSelectorBuilder;
        var producerSelector = builder.Build(this.producers.Select(p => p.Value));
        this.faultTolerantProducer = new KafkaFaultTolerantProducer(
            this.producers.Values.ToList(),
            new FaultTolerantProducer(producerSelector, Configuration.FailoverAttempts));
    }

    /// <summary>
    ///   Отправляет сообщение с полностью заданными заголовками, минуя их сборку из
    ///   <see cref="PublishingOptions"/>. Нужно для ответов: они должны нести корреляционный
    ///   идентификатор своего запроса, а не подхваченный из ambient-хранилища.
    /// </summary>
    internal Task<EmitResult> SendRaw<TResponse>(MessageLabel label, TResponse payload, IDictionary<string, object> headers)
        where TResponse : class
    {
        return this.faultTolerantProducer.Send(new Message<TResponse>(label, payload, headers), new PublishingOptions());
    }

    private KafkaProducer BuildProducer(string url)
    {
        this.Logger.LogTrace("Enlisting a new producer of [{Label}] at URL=[{Url}]...", Configuration.Label, url);
        var producer = new KafkaProducer(Bus, url, producerFactory, Configuration);

        if (Configuration.RequiresCallback)
        {
            producer.WithCallback();
            this.Logger.LogTrace("A producer of [{Label}] at URL=[{BrokerUrl}] will listen for replies in [{CallbackTopic}]", producer.Label, producer.BrokerUrl, Configuration.CallbackTopic);
        }

        this.producers.TryAdd(producer.Id, producer);
        this.Logger.LogTrace("A producer of [{Label}] at URL=[{BrokerUrl}] has been enlisted", producer.Label, producer.BrokerUrl);

        return producer;
    }
}
