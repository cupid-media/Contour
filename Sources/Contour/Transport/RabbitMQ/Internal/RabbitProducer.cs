using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Contour.Configuration;
using Contour.Helpers;
using Contour.Receiving;
using Contour.Sending;
using Contour.Sending.Exceptions;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Topology;
using RabbitMQ.Client;

namespace Contour.Transport.RabbitMQ.Internal;

/// <summary>
/// Отправитель сообщений.
/// Отправитель создается для конкретной метки сообщения и конкретной конечной точки.
/// В случае отправки запроса с ожиданием ответа, отправитель создает получателя ответного сообщения.
/// </summary>
internal sealed class RabbitProducer : IProducer
{
    private readonly ILogger logger;
    private readonly RabbitBus bus;
    private readonly IConnectionPool<IRabbitConnection> connectionPool;
    private readonly SemaphoreSlim semaphoreSlim = new(1, 1);
    private readonly CancellationTokenSource workCts = new();
    private IPublishConfirmationTracker confirmationTracker;
    private IRabbitConnection currentConnection;
    private bool isPrepared;
    private bool isStarted;
    private bool withCallback;

    public RabbitProducer(
        RabbitBus bus,
        string brokerUrl,
        IConnectionPool<IRabbitConnection> connectionPool,
        RabbitSenderOptions senderOptions)
    {
        Id = Guid.NewGuid();
        this.bus = bus;

        this.connectionPool = connectionPool;
        BrokerUrl = brokerUrl;
        Label = senderOptions.Label;
        SenderOptions = senderOptions;

        this.logger = LogManager.GetLogger($"{GetType().FullName}({BrokerUrl}, {Label}, {Id})");
    }

    public Guid Id { get; }

    public bool IsInGoodCondition => this.currentConnection?.IsBlocked == false && (Channel?.IsOpen ?? false);

    public MessageLabel Label { get; }

    public string BrokerUrl { get; }

    public string ConnectionKey => Channel.ConnectionKey;

    private RabbitReceiver CallbackReceiver { get; set; }

    private RabbitReceivingSource CallbackReceivingSource { get; set; }

    private RabbitChannel Channel { get; set; }

    private RabbitSenderOptions SenderOptions { get; }

    private IRouteResolver RouteResolver { get; set; }

    public async ValueTask DisposeAsync()
    {
        this.logger.LogTrace("Disposing producer of [{Label}]", Label);
        UnsubscribeFromAllEvents();
        if (Channel != null)
        {
            try
            {
                await Channel.DisposeAsync();
            }
            catch (Exception)
            {
                // Any channel/model disposal exceptions are suppressed
            }
        }

        if (CallbackReceiver != null)
        {
            await CallbackReceiver.DisposeAsync();
        }

        this.confirmationTracker?.Dispose();
        this.semaphoreSlim.Dispose();
        this.workCts.Dispose();
    }

    /// <summary>
    /// Отправляет сообщение <paramref name="message"/> в шину сообщений.
    /// </summary>
    /// <param name="message">
    /// Отправляемое сообщение.
    /// </param>
    /// <returns>
    /// Задача, которая отправляет сообщение в шину сообщений.
    /// </returns>
    public async Task<EmitResult> Send<TRequest>(Message<TRequest> message) where TRequest : class
    {
        if (this.semaphoreSlim.CurrentCount > 0)
        {
            try
            {
                var nativeRoute = (RabbitRoute)await RouteResolver.Resolve(this.bus.Endpoint, message.Label);
                this.logger.LogTrace("Emitting message [{Label}] through [{Route}]", message.Label, nativeRoute);
                Func<IBasicProperties, IDictionary<string, object>> propsVisitor = p => ExtractProperties(ref p, message.Headers);

                if (SenderOptions.ConfirmationRequired)
                {
                    Task confirmationTask = null;
                    await Channel.PublishWithConfirmation(nativeRoute, message, nextSequenceNumber =>
                    {
                        confirmationTask = this.confirmationTracker.Track(nextSequenceNumber);
                    }, propsVisitor, CancellationToken.None);

                    if (confirmationTask != null)
                    {
                        await confirmationTask;
                    }
                }
                else
                {
                    await Channel.Publish(nativeRoute, message, propsVisitor, CancellationToken.None);
                }

                return new EmitResult
                {
                    UsedConnectionString = BrokerUrl
                };
            }
            catch (Exception e)
            {
                throw new FailedToSendMessageException(e.Message, e);
            }
        }

        throw new FailedToSendMessageException($"Unable to publish via producer [{GetHashCode()}] because it is not yet started or is recovering");
    }

    /// <summary>
    /// Выполняет запрос в шину сообщений передавая сообщение <see cref="IMessage"/> и ожидает ответ с типом TResponse
    /// </summary>
    /// <param name="request">
    /// Информация передаваемая как запрос.
    /// </param>
    /// <returns>
    /// Возвращает задачу, которая выполняет запрос в шину сообщения.
    /// </returns>
    /// <exception cref="Exception">
    /// Исключение генерируемое при возникновении сбоя во время отправки запроса.
    /// </exception>
    /// <exception cref="MessageRejectedException">
    /// Исключение генерируется, если нельзя отправить запрос в шину сообщений.
    /// </exception>
    public async Task<RequestResult<TResponse>> Request<TRequest, TResponse>(Message<TRequest> request) where TRequest : class where TResponse : class
    {
        request.Headers[Headers.ReplyRoute] = await ResolveCallbackRoute();
        var responseTimeout = Headers.Extract<TimeSpan?>(request.Headers, Headers.Timeout);
        var correlationId = (string)request.Headers[Headers.CorrelationId];

        var responseTask = CallbackReceivingSource.Expect<TResponse>(correlationId, responseTimeout);

        var sendResult = await Send(request);
        await responseTask;

        return new RequestResult<TResponse>
        {
            Response = (await responseTask).Payload,
            UsedConnectionString = sendResult.UsedConnectionString
        };
    }

    public async Task Prepare(CancellationToken token)
    {
        await this.semaphoreSlim.ExecuteWithCondition(async () =>
        {
            this.logger.LogTrace("Preparing producer [{ProducerHashCode}] of [{Label}]", GetHashCode(), Label);

            this.currentConnection = await this.connectionPool.Get(BrokerUrl, SenderOptions.ReuseConnection, token);

            if (RouteResolver == null || SenderOptions.RecreateRouteOnFailure)
            {
                await BuildRouteResolver(token);
            }

            if (SenderOptions.ConfirmationRequired)
            {
                Channel = await this.currentConnection.OpenSharedChannel("producer-confirm", new CreateChannelOptions(true, true), token);
                this.confirmationTracker = new PublishConfirmationTracker(SenderOptions.ConfirmationTimeout);
                await Channel.OnConfirmation(this.confirmationTracker.HandleConfirmation, token);
            }
            else
            {
                Channel = await this.currentConnection.OpenSharedChannel("producer-nonconfirm", cancellationToken: token);
                this.confirmationTracker = new DummyPublishConfirmationTracker();
            }

            Channel.Shutdown.Event += OnChannelShutdown;

            if (this.withCallback)
            {
                await PrepareCallbackReceiver(token);
            }

            this.logger.LogTrace("Producer of [{Label}] prepared successfully", Label);
            this.isPrepared = true;

            return 0;
        }, () => !this.isPrepared, logger: this.logger, cancellationToken: token);
    }

    /// <summary>
    /// Запускает отправитель, после этого можно получать ответы на запросы.
    /// </summary>
    public async Task Start(CancellationToken token)
    {
        if (!this.isPrepared)
        {
            this.logger.LogWarning("Producer should be prepared before start");
            return;
        }

        await this.semaphoreSlim.ExecuteWithCondition(async () =>
        {
            this.logger.LogTrace("Starting producer [{ProducerHashCode}] of [{Label}]", GetHashCode(), Label);

            if (CallbackReceivingSource != null)
            {
                await CallbackReceivingSource.StartConsuming(token);
            }

            this.logger.LogTrace("Producer of [{Label}] started successfully", Label);
            this.isStarted = true;

            return 0;
        }, () => !this.isStarted && this.isPrepared, logger: this.logger, cancellationToken: token);
    }

    /// <summary>
    /// Останавливает отправитель, после этого ответы на запросы не будут обрабатываться.
    /// </summary>
    public async Task Stop(CancellationToken token)
    {
        await InternalStop(OperationStopReason.Regular, token);
    }

    public void WithCallback()
    {
        this.withCallback = true;
    }

    private async Task BuildRouteResolver(CancellationToken token)
    {
        this.logger.LogTrace("Using connection [{ConnectionId}] at URL=[{BrokerUrl}] to resolve a producer", this.currentConnection.Id, BrokerUrl);

        using var topologyBuilder = new RabbitTopologyBuilder(this.currentConnection);
        var builder = new RabbitRouteResolverBuilderParams
        {
            Endpoint = this.bus.Endpoint,
            Options = SenderOptions,
            Topology = topologyBuilder
        };
        var routeResolverBuilderFunc = SenderOptions.RouteResolverBuilder;

        if (routeResolverBuilderFunc == null)
        {
            throw new InvalidOperationException($"RouteResolverBuilder must be set for [{SenderOptions.Label}]");
        }

        RouteResolver = await routeResolverBuilderFunc(builder, token);
    }

    private async Task PrepareCallbackReceiver(CancellationToken token)
    {
        CallbackReceiver = this.bus.RegisterReceiver(SenderOptions.CallbackOptions with
        {
            ConnectionString = BrokerUrl,
            ConnectionStringProvider = null
        }, true);
        await CallbackReceiver.Prepare(token);
        CallbackReceivingSource = CallbackReceiver.ReceivingSources.FirstOrDefault(l => l.BrokerUrl == BrokerUrl);
        if (CallbackReceivingSource == null)
        {
            throw new BusConfigurationException($"Unable to find a suitable listener for receiver {this}");
        }
        CallbackReceivingSource.StopOnChannelShutdown = true;
        CallbackReceivingSource.OnStoppedAsync.Event += OnCallbackReceiverStoppedAsync;
    }

    private async Task OnCallbackReceiverStoppedAsync(ReceivingSourceStoppedEventArgs e)
    {
        UnsubscribeFromAllEvents();
        this.isPrepared = false;
        this.isStarted = false;
        if (e.Reason != OperationStopReason.Regular)
        {
            await Recreate();
        }
    }

    /// <summary>
    /// Устанавливает заголовки сообщения в свойства сообщения.
    /// </summary>
    /// <param name="props">
    /// Свойства сообщения, куда устанавливаются заголовки.
    /// </param>
    /// <param name="sourceHeaders">
    /// The source Headers.
    /// </param>
    /// <returns>
    /// The <see cref="IDictionary{K,V}"/>.
    /// </returns>
    private static IDictionary<string, object> ExtractProperties(ref IBasicProperties props, IDictionary<string, object> sourceHeaders)
    {
        var headers = new Dictionary<string, object>(sourceHeaders);

        var persist = Headers.Extract<bool?>(headers, Headers.Persist);
        var ttl = Headers.Extract<TimeSpan?>(headers, Headers.Ttl);
        var correlationId = Headers.Extract<string>(headers, Headers.CorrelationId);
        var replyRoute = Headers.Extract<RabbitRoute>(headers, Headers.ReplyRoute);

        if (persist.HasValue && persist.Value)
        {
            props.DeliveryMode = DeliveryModes.Persistent;
        }

        if (ttl.HasValue)
        {
            props.Expiration = Math.Round(ttl.Value.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        }

        if (correlationId != null)
        {
            props.CorrelationId = correlationId;
        }

        if (replyRoute != null)
        {
            props.ReplyToAddress = new PublicationAddress("direct", replyRoute.Exchange, replyRoute.RoutingKey);
        }

        return headers;
    }

    private void UnsubscribeFromAllEvents()
    {
        if (Channel != null)
        {
            Channel.Shutdown.Event -= OnChannelShutdown;
        }

        if (CallbackReceivingSource != null)
        {
            CallbackReceivingSource.OnStoppedAsync.Event -= OnCallbackReceiverStoppedAsync;
        }
    }

    private async Task InternalStop(OperationStopReason reason, CancellationToken cancellationToken)
    {
        await this.semaphoreSlim.ExecuteWithCondition(async () =>
        {
            this.isStarted = false;
            this.logger.LogTrace("Stopping producer of [{Label}]. Reason: [{Reason}]", Label, reason);

            this.workCts.Cancel();

            if (CallbackReceivingSource != null)
            {
                await CallbackReceivingSource.StopConsuming(cancellationToken);
            }

            this.logger.LogTrace("Producer of [{Label}] stopped successfully", Label);

            return 0;
        }, () => this.isStarted, logger: this.logger, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Определяет маршрут ответного сообщения.
    /// </summary>
    /// <returns>
    /// Маршрут ответного сообщения.
    /// </returns>
    /// <exception cref="BusConfigurationException">
    /// Генерируется в случае, если нет конечной точки, которая может отправить сообщение с такой меткой или в случае, если нет ни получателя маршрута ответных сообщений.
    /// </exception>
    private ValueTask<IRoute> ResolveCallbackRoute()
    {
        if (CallbackReceivingSource == null)
        {
            throw new FailedToSendMessageException($"No reply endpoint is defined for publisher of [{Label}].");
        }

        if (CallbackReceivingSource.Endpoint.CallbackRouteResolver == null)
        {
            throw new FailedToSendMessageException(
                $"No callback route resolver is defined for listener on [{CallbackReceivingSource.Endpoint.ListeningSource}].");
        }

        return CallbackReceivingSource.Endpoint.CallbackRouteResolver.Resolve(this.bus.Endpoint, MessageLabel.Any);
    }

    private async Task OnChannelShutdown(RabbitChannelShutdownEventArgs eventArgs)
    {
        this.logger.LogTrace("Channel shutdown details: {ShutdownEventArgs}", eventArgs.ShutdownEventArgs);

        try
        {
            UnsubscribeFromAllEvents();
            this.isPrepared = false;
            this.isStarted = false;

            if (eventArgs.ShutdownEventArgs.Initiator != ShutdownInitiator.Application)
            {
                this.logger.LogInformation("The producer will be recreated");
                await Recreate();
            }
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Error while handling channel shutdown. Channel shutdown details: {ShutdownEventArgs}", eventArgs.ShutdownEventArgs);
        }
    }

    private async Task Recreate()
    {
        var token = this.workCts.Token;
        while (!token.IsCancellationRequested)
        {
            try
            {
                if (CallbackReceiver != null)
                {
                    await CallbackReceiver.Stop(token);
                    await CallbackReceiver.DisposeAsync();
                    CallbackReceiver = null;
                    CallbackReceivingSource = null;
                }
                await Prepare(token);
                await Start(token);
                break;
            }
            catch (Exception e)
            {
                this.logger.LogError(e, "Failed to recreate producer. Retry in 1 second");
                await Task.Delay(TimeSpan.FromSeconds(1), token);
            }
        }
    }
}
