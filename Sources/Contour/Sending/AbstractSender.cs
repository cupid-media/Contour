using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Sending;

/// <summary>
/// Отправитель, который не знает о транспортном уровне.
/// </summary>
internal abstract class AbstractSender<TConfiguration, TBus, TProducer> : ISender
    where TConfiguration : ISenderConfiguration
    where TBus : IBus
    where TProducer : IProducer
{
    protected readonly ILogger Logger;

    protected AbstractSender(TBus bus, TConfiguration configuration)
    {
        Bus = bus;
        Configuration = configuration;
        this.Logger = LogManager.GetLogger($"{GetType().FullName}({Bus.Endpoint}, {Configuration.Label})");
    }

    protected TBus Bus { get; }

    protected TConfiguration Configuration { get; private set; }

    protected abstract ICollection<TProducer> Producers { get; }

    public virtual bool IsHealthy => this.isStarted;

    private bool isConfigured;

    private bool isStarted;

    private readonly ConcurrentDictionary<string, bool> reportedUnsupportedOptions = new();

    /// <summary>
    /// Проверяет возможность создать маршрут для метки сообщения.
    /// </summary>
    /// <param name="label">Метка сообщения, для которой нужно создать маршрут.</param>
    /// <returns><c>true</c> - если можно создать маршрут.</returns>
    public bool CanRoute(MessageLabel label)
    {
        return label.IsAlias ? label.Name.Equals(Configuration.Key) : label.Equals(Configuration.Label);
    }

    public async Task Prepare(CancellationToken cancellationToken)
    {
        if (!this.isConfigured)
        {
            await PrepareProducers(cancellationToken);
            this.isConfigured = true;
        }
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        if (this.isConfigured && !this.isStarted)
        {
            this.Logger.LogTrace("Starting sender of [{Label}]", Configuration.Label);
            await StartProducers(cancellationToken);
            this.isStarted = true;
        }
        else if (!this.isConfigured)
        {
            this.Logger.LogWarning("Sender cannot be started because it's not configured");
        }
    }

    public async Task Stop(CancellationToken cancellationToken)
    {
        if (this.isStarted)
        {
            this.Logger.LogTrace("Stopping sender of [{Label}]", Configuration.Label);
            await StopProducers(cancellationToken);
        }

        this.isConfigured = false;
        this.isStarted = false;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var producer in Producers)
        {
            await producer.DisposeAsync();
        }
        ClearProducers();
    }

    /// <summary>
    /// Sends message using request-reply pattern.
    /// Copies all allowed message headers. And generates new <see cref="Headers.CorrelationId"/> header.
    /// </summary>
    /// <param name="label">Message label.</param>
    /// <param name="payload">Message payload.</param>
    /// <param name="options">Request options.</param>
    /// <returns>Request processing task.</returns>
    public async Task<RequestResult<TResponse>> Request<TRequest, TResponse>(MessageLabel label, TRequest payload, RequestOptions options) where TRequest : class where TResponse : class
    {
        WarnAboutUnsupportedOptions(options);

        var headers = ApplyRequestingOptions(options);
        var messageLabel = Configuration.Label.Equals(MessageLabel.Any) ? label : Configuration.Label;
        var message = new Message<TRequest>(messageLabel, payload, headers) { Key = options.Key };

        this.Logger.LogTrace("Requesting: label=[{MessageLabel}], correlationId=[{CorrelationId}]", messageLabel, headers[Headers.CorrelationId]);
        return await InternalRequest<TRequest, TResponse>(message, options);
    }

    /// <summary>
    /// Отправляет одностороннее сообщение.
    /// </summary>
    /// <param name="label">Метка отправляемого сообщения.</param>
    /// <param name="payload">Тело сообщения.</param>
    /// <param name="options">Заголовки сообщения.</param>
    /// <returns>Задача выполнения отправки сообщения.</returns>
    public async Task<EmitResult> Send<TRequest>(MessageLabel label, TRequest payload, PublishingOptions options) where TRequest : class
    {
        WarnAboutUnsupportedOptions(options);

        var headers = ApplyPublishingOptions(options);
        var messageLabel = Configuration.Label.Equals(MessageLabel.Any) ? label : Configuration.Label;
        var message = new Message<TRequest>(messageLabel, payload, headers) { Key = options.Key };

        this.Logger.LogTrace("Sending: label=[{MessageLabel}], correlationId=[{CorrelationId}]", messageLabel, headers[Headers.CorrelationId]);
        return await InternalSend(message, options);
    }

    /// <summary>
    /// Фильтр обработки сообщения, который отсылает сообщение.
    /// </summary>
    /// <returns>Задача выполнения фильтра.</returns>
    protected abstract Task<EmitResult> InternalSend<TRequest>(Message<TRequest> message, PublishingOptions options) where TRequest : class;

    protected abstract Task<RequestResult<TResponse>> InternalRequest<TRequest, TResponse>(Message<TRequest> message, RequestOptions options) where TRequest : class where TResponse : class;

    protected abstract Task PrepareProducers(CancellationToken cancellationToken);

    /// <summary>
    /// Заданные в <paramref name="options"/> настройки, которые этот отправитель не поддерживает.
    /// </summary>
    protected virtual IEnumerable<string> GetUnsupportedOptions(PublishingOptions options)
    {
        return [];
    }

    protected abstract Task StopProducers(CancellationToken cancellationToken);

    protected abstract Task StartProducers(CancellationToken cancellationToken);

    protected abstract void ClearProducers();

    /// <summary>
    /// Пишет предупреждение о заданных настройках, которые отправитель не поддерживает.
    /// </summary>
    /// <remarks>
    /// Предупреждение пишется один раз на настройку: отправка идёт на каждое сообщение, и лог иначе забьётся.
    /// </remarks>
    private void WarnAboutUnsupportedOptions(PublishingOptions options)
    {
        foreach (var option in GetUnsupportedOptions(options))
        {
            if (this.reportedUnsupportedOptions.TryAdd(option, true))
            {
                this.Logger.LogWarning("Option [{Option}] is set, but the sender of [{Label}] does not support it: the option has no effect", option, Configuration.Label);
            }
        }
    }

    /// <summary>
    /// Конвертирует настройки публикации сообщения в заголовки сообщения.
    /// </summary>
    /// <param name="options">Настройки публикации сообщения.</param>
    /// <returns>Заголовки сообщения.</returns>
    private IDictionary<string, object> ApplyPublishingOptions(PublishingOptions options)
    {
        var storage = Configuration.IncomingMessageHeaderStorage;
        var inputHeaders = storage.Load() ?? new Dictionary<string, object>();
        var outputHeaders = new Dictionary<string, object>(inputHeaders);

        Headers.ApplyBreadcrumbs(outputHeaders, Bus.Endpoint.Address, options.BreadcrumbsPrefix);
        Headers.ApplyOriginalMessageId(outputHeaders);

        Headers.ApplyPersistently(outputHeaders, options.Persistently ?? Configuration.Persistently);

        if (Configuration.Delayed)
        {
            Headers.ApplyDelay(outputHeaders, options.Delay);
        }

        Headers.ApplyTtl(outputHeaders, options.Ttl ?? Configuration.Ttl);
        Headers.ApplyAdditionalHeaders(outputHeaders, options.AdditionalHeaders);
        Headers.ApplySentTimestamp(outputHeaders);

        if (!outputHeaders.ContainsKey(Headers.CorrelationId))
        {
            outputHeaders[Headers.CorrelationId] = Guid.NewGuid().ToString("n");
        }

        return outputHeaders;
    }

    /// <summary>
    /// Конвертирует настройки публикации сообщения в заголовки сообщения.
    /// </summary>
    /// <param name="requestOptions">Настройки публикации сообщения.</param>
    /// <returns>Заголовки сообщения.</returns>
    private IDictionary<string, object> ApplyRequestingOptions(RequestOptions requestOptions)
    {
        var headers = ApplyPublishingOptions(requestOptions as PublishingOptions);

        var timeout = requestOptions.Timeout ?? Configuration.RequestTimeout;
        if (timeout != null)
        {
            headers[Headers.Timeout] = timeout.Value;
        }

        headers[Headers.CorrelationId] = Guid.NewGuid().ToString("n");

        return headers;
    }
}
