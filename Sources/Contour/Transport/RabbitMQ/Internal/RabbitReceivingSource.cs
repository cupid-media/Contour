using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Contour.Helpers;
using Contour.Helpers.Timing;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Topology;
using RabbitMQ.Client.Events;

namespace Contour.Transport.RabbitMQ.Internal;

internal sealed class RabbitReceivingSource : IReceivingSource
{
    private readonly ConcurrentDictionary<string, MessageExpectation> expectations = new();
    private readonly object expectationsLock = new();
    private readonly SemaphoreSlim semaphore = new(1, 1);
    private readonly ILogger logger;
    private readonly RabbitBus bus;
    private readonly IConnectionPool<IRabbitConnection> connectionPool;
    private readonly CancellationTokenSource receivingTokenSource;
    private readonly CancellationToken receivingToken;

    private ConsumingAction consumingAction;
    private object registeredConsumer;

    private bool isPrepared;
    private bool isConsuming;

    private delegate Task ConsumingAction(RabbitDelivery delivery);

    /// <summary>
    /// Таймер, который отслеживает, что время ожидания ответа на запрос вышло.
    /// </summary>
    private ITicketTimer ticketTimer;
    private readonly List<RabbitConsumer> consumers = [];

    public RabbitReceivingSource(
        RabbitBus bus,
        string brokerUrl,
        IConnectionPool<IRabbitConnection> connectionPool,
        RabbitReceiverOptions receiverOptions)
    {
        Id = Guid.NewGuid();
        this.bus = bus;
        this.connectionPool = connectionPool;

        ReceiverOptions = receiverOptions;
        BrokerUrl = brokerUrl;

        this.receivingTokenSource = new CancellationTokenSource();
        this.receivingToken = this.receivingTokenSource.Token;

        this.logger = LogManager.GetLogger($"{GetType().FullName}({BrokerUrl}, {ReceiverOptions.Label}, {Id})");
    }

    public Guid Id { get; }

    public AsyncEventWrapper<ReceivingSourceStoppedEventArgs> OnStoppedAsync { get; } = new();

    public event Action<IReceivingSource> OnStartConsuming;

    public event Action<IReceivingSource> OnFinishConsuming;

    public IListeningSource ListeningSource => Endpoint.ListeningSource;

    public ISubscriptionEndpoint Endpoint { get; private set; }

    public RabbitReceiverOptions ReceiverOptions { get; }

    public string BrokerUrl { get; }

    /// <summary>
    /// Denotes if a receiving source should dispose itself if any channel related errors have occurred
    /// </summary>
    public bool StopOnChannelShutdown { get; set; }

    /// <summary>
    /// Создает входящее сообщение.
    /// </summary>
    /// <param name="deliveryChannel">
    /// The delivery Channel.
    /// </param>
    /// <param name="args">
    /// Аргументы, с которыми получено сообщение.
    /// </param>
    /// <returns>
    /// Входящее сообщение.
    /// </returns>
    public RabbitDelivery BuildDeliveryFrom(RabbitChannel deliveryChannel, BasicDeliverEventArgs args)
    {
        return new RabbitDelivery(this.bus, deliveryChannel, args, ReceiverOptions.AcceptRequired);
    }

    /// <summary>
    /// Возвращает задачу, с ожиданием ответа на запрос.
    /// </summary>
    /// <param name="correlationId">
    /// Корреляционный идентификатор, с помощью которого определяется принадлежность ответа определенному запросу.
    /// </param>
    /// <param name="timeout">
    /// Время ожидания ответа.
    /// </param>
    /// <returns>
    /// Задача ожидания ответа на запрос.
    /// </returns>
    public async Task<Message<TResponse>> Expect<TResponse>(string correlationId, TimeSpan? timeout) where TResponse : class
    {
        if (this.expectations.TryGetValue(correlationId, out var expectation))
        {
            return (Message<TResponse>) await expectation.Task;
        }

        // Используется блокировка, чтобы гарантировать, что метод CreateExpectation будет вызван один раз, т.к. это не гарантируется реализацией метода GetOrAdd.
        lock (this.expectationsLock)
        {
            expectation = this.expectations.GetOrAdd(correlationId, c => CreateExpectation<TResponse>(c, timeout));
        }

        return (Message<TResponse>) await expectation.Task;
    }

    /// <summary>
    /// Добавляет еще одного обработчика сообщений.
    /// </summary>
    /// <param name="consumer">
    /// Обработчик сообщения.
    /// </param>
    /// <typeparam name="T">
    /// Тип входящего сообщения.
    /// </typeparam>
    public void RegisterConsumer<T>(IConsumer<T> consumer) where T : class
    {
        this.registeredConsumer = consumer;

        if (consumer is IConsumerOf<T>)
        {
            this.consumingAction = delivery =>
            {
                IConsumingContext<T> context = delivery.BuildConsumingContext<T>(ReceiverOptions.Label);

                return ((IConsumerOf<T>)consumer).HandleAsync(context);
            };
        }
        else if (consumer is IAsyncConsumerOf<T>)
        {
            this.consumingAction = delivery =>
            {
                IConsumingContext<T> context = delivery.BuildConsumingContext<T>(ReceiverOptions.Label);

                return ((IAsyncConsumerOf<T>)consumer).HandleAsync(context);
            };
        }
        else
        {
            throw new ArgumentException($"Parameter consumer mast realize IConsumerOf<T> or IAsyncConsumerOf<T>, type: {consumer.GetType().Name}", nameof(consumer));
        }
    }

    public async Task Prepare(CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.receivingToken);
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            if (Endpoint == null || ReceiverOptions.RecreateEndpointOnFailure)
            {
                await BuildEndpoint(linkedCts.Token);
            }

            this.logger.LogDebug("Prepare consuming on [{ListeningSource}]", ListeningSource);
            var count = (int)ReceiverOptions.ParallelismLevel;
            var token = linkedCts.Token;

            var tasks = Enumerable
                .Range(this.consumers.Count, Math.Max(count - this.consumers.Count, 0))
                .Select(_ => InitializeConsumer(token));
            await Task.WhenAll(tasks);

            this.isPrepared = true;
            return 0;
        }, () => !this.isPrepared, logger: this.logger, linkedCts.Token);
    }

    public async Task StartConsuming(CancellationToken cancellationToken)
    {
        if (!this.isPrepared)
        {
            this.logger.LogWarning("Cannot start consuming before preparation");
            return;
        }

        await this.semaphore.ExecuteWithCondition(async () =>
        {
            this.logger.LogDebug("Starting consuming on [{ListeningSource}]", ListeningSource);
            this.ticketTimer ??= new RoughTicketTimer(TimeSpan.FromSeconds(1));

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.receivingToken);

            var tasks = this.consumers.Where(c => !c.IsConsuming).Select(c => StartConsumingAsync(c, linkedCts.Token)).ToList();
            await Task.WhenAll(tasks);

            this.isConsuming = true;
            this.logger.LogTrace("Receiving source's workers started successfully");
            return 0;
        }, () => !this.isConsuming && this.isPrepared, logger: this.logger, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Останавливает обработку входящих сообщений.
    /// </summary>
    public async Task StopConsuming(CancellationToken cancellationToken)
    {
        await InternalStop(OperationStopReason.Regular, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        this.receivingTokenSource.Dispose();
        this.ticketTimer?.Dispose();

        await this.semaphore.ExecuteWithCondition(async () =>
        {
            foreach (var rabbitConsumer in this.consumers)
            {
                try
                {
                    rabbitConsumer.Channel.Shutdown.Event -= OnChannelShutdown;
                    await rabbitConsumer.Channel.DisposeAsync();
                }
                catch (Exception)
                {
                    // Any channel/model disposal exceptions are suppressed
                }
            }

            this.consumers.Clear();
            return 0;
        }, logger: this.logger);
    }

    /// <summary>
    /// Доставляет сообщение до обработчика.
    /// </summary>
    /// <param name="delivery">
    /// Входящее сообщение.
    /// </param>
    private async Task Deliver(RabbitDelivery delivery)
    {
        DiagnosticProps.Store(DiagnosticProps.Names.ConsumerConnectionString, delivery.Channel.ConnectionString);
        using var _ = this.logger.BeginScope(("ConsumerConnectionString", delivery.Channel.ConnectionString));

        IDisposable originalMsgIdScope = null;
        IDisposable breadcrumbsScope = null;
        try
        {
            this.logger.LogTrace("Received delivery labeled [{Label}] from exchange [{Exchange}] with consumer [{ConsumerTag}]", delivery.Label, delivery.Args.Exchange, delivery.Args.ConsumerTag);

            if (delivery.Headers.ContainsKey(Headers.OriginalMessageId))
            {
                var originalMessageId = Headers.GetString(delivery.Headers, Headers.OriginalMessageId);

                DiagnosticProps.Store(DiagnosticProps.Names.OriginalMessageId, originalMessageId);
                originalMsgIdScope = this.logger.BeginScope(("OriginalMessageId", originalMessageId));
                this.logger.LogTrace("Сквозной идентификатор сообщения [{OriginalMessageId}]", originalMessageId);
            }

            if (delivery.Headers.ContainsKey(Headers.Breadcrumbs))
            {
                var breadcrumbs = Headers.GetString(delivery.Headers, Headers.Breadcrumbs);

                DiagnosticProps.Store(DiagnosticProps.Names.Breadcrumbs, breadcrumbs);
                breadcrumbsScope = this.logger.BeginScope(("Breadcrumbs", breadcrumbs));
                this.logger.LogTrace("Сообщение было обработано в конечных точках: [{Breadcrumbs}]", breadcrumbs);
            }

            var stopwatch = Stopwatch.StartNew();

            try
            {
                // тут не происходит ничего, что можно было бы сделать асинхронно
                var processed = TryHandleAsResponse(delivery);

                if (!processed)
                {
                    processed = await TryHandleAsSubscription(delivery);
                }

                if (!processed)
                {
                    await OnUnhandled(delivery);
                }
            }
            catch (Exception ex)
            {
                await OnFailure(delivery, ex);
            }

            stopwatch.Stop();
            this.logger.LogTrace("Message labeled [{Label}] processed in {ElapsedMilliseconds} ms", delivery.Label, stopwatch.ElapsedMilliseconds);
        }
        finally
        {
            originalMsgIdScope?.Dispose();
            breadcrumbsScope?.Dispose();
        }
    }

    private async Task BuildEndpoint(CancellationToken cancellationToken)
    {
        var connection = await this.connectionPool.Get(BrokerUrl, ReceiverOptions.ReuseConnection, cancellationToken);
        this.logger.LogTrace("Using connection [{ConnectionId}] at URL=[{BrokerUrl}] to resolve a ReceivingSource", connection.Id, BrokerUrl);

        using var topologyBuilder = new RabbitTopologyBuilder(connection);
        var endpointBuilder = ReceiverOptions.EndpointBuilder;
        if (endpointBuilder == null)
        {
            throw new InvalidOperationException($"EndpointBuilder is null for [{ReceiverOptions.Label}].");
        }

        Endpoint = await endpointBuilder(new RabbitSubscriptionEndpointBuilderParams
        {
            Endpoint = this.bus.Endpoint,
            ReceiverOptions = ReceiverOptions,
            Topology = topologyBuilder
        }, cancellationToken);
    }

    private async Task InternalStop(OperationStopReason reason, CancellationToken cancellationToken)
    {
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            this.isConsuming = false;
            this.isPrepared = false;
            this.logger.LogInformation("Stopping consuming on [{ListeningSource}]. Reason: [{Reason}]", ListeningSource, reason);

            this.receivingTokenSource.Cancel();
            await Task.WhenAll(this.consumers.Select(c => c.StopConsuming(cancellationToken)));

            if (this.registeredConsumer is IAsyncDisposable disposable)
            {
                await disposable.DisposeAsync();
            }

            this.expectations.Values.ForEach(e => e.Cancel());
            this.expectations.Clear();

            await OnStoppedAsync.Invoke(new ReceivingSourceStoppedEventArgs(this, reason, cancellationToken));
            this.logger.LogTrace("ReceivingSource stopped successfully");

            return 0;
        }, () => this.isConsuming || this.isPrepared, logger: this.logger, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Формирует ответ на запрос.
    /// </summary>
    /// <param name="delivery">
    /// Входящее сообщение, которое является ответом.
    /// </param>
    /// <returns>
    /// Сообщение с ответом на запрос.
    /// </returns>
    private Message<TResponse> BuildResponse<TResponse>(IDelivery delivery) where TResponse : class
    {
        return delivery.UnpackAs<TResponse>();
    }

    /// <summary>
    /// Создаёт ожидание ответа на запрос.
    /// </summary>
    /// <param name="correlationId">Корреляционный идентификатор, с помощью которого определяется принадлежность ответа определенному запросу.</param>
    /// <param name="timeout">Время ожидания ответа.</param>
    /// <returns>Ожидание ответа на запрос.</returns>
    private MessageExpectation CreateExpectation<TResponse>(string correlationId, TimeSpan? timeout) where TResponse : class
    {
        long? timeoutTicket = null;
        if (timeout.HasValue)
        {
            timeoutTicket = this.ticketTimer.Acquire(
                timeout.Value,
                () =>
                {
                    OnResponseTimeout(correlationId);
                });
        }

        return new MessageExpectation(BuildResponse<TResponse>, timeoutTicket);
    }

    private async Task InitializeConsumer(CancellationToken token)
    {
        var connection = await this.connectionPool.Get(BrokerUrl, ReceiverOptions.ReuseConnection, token);
        var channel = await connection.OpenChannel(token: token);
        channel.Shutdown.Event += OnChannelShutdown;

        if (ReceiverOptions.QoS != null)
        {
            await channel.SetQos(ReceiverOptions.QoS, token);
        }

        var consumer = channel.BuildConsumer();
        var rabbitConsumer = new RabbitConsumer(consumer, channel);
        this.consumers.Add(rabbitConsumer);
    }

    private async Task StartConsumingAsync(RabbitConsumer rabbitConsumer, CancellationToken token)
    {
        await rabbitConsumer.StartConsuming(ListeningSource, ReceiverOptions.AcceptRequired,
            async (_, args) =>
            {
                if (token.IsCancellationRequested)
                {
                    this.logger.LogWarning("Token was cancelled, but receiving source still receive messages. Routing key [{RoutingKey}], exchange [{Exchange}]", args.RoutingKey, args.Exchange);
                    return;
                }

                OnStartConsuming?.Invoke(this);

                RabbitDelivery delivery;
                try
                {
                    delivery = BuildDeliveryFrom(rabbitConsumer.Channel, args);
                }
                catch (Exception e)
                {
                    this.logger.LogCritical(e, "Delivery object has not been constructed, fetch a follow messages of the consumer for '{Address}' is impossible", ListeningSource.Address);
                    OnFinishConsuming?.Invoke(this);
                    throw;
                }

                try
                {
                    await Deliver(delivery);
                }
                catch (Exception e)
                {
                    await OnFailure(delivery, e);
                }
                finally
                {
                    OnFinishConsuming?.Invoke(this);
                }
            },
            token);

        this.logger.LogTrace("A consumer tagged [{Tag}] has been registered in receiving source of [{Label}]", rabbitConsumer.Tag, ReceiverOptions.Label);
        this.logger.LogDebug("Start consuming");
    }

    private async Task OnChannelShutdown(RabbitChannelShutdownEventArgs eventArgs)
    {
        this.logger.LogTrace("Channel shutdown details: {ShutdownEventArgs}", eventArgs.ShutdownEventArgs);

        try
        {
            eventArgs.RabbitChannel.Shutdown.Event -= OnChannelShutdown;
            this.isPrepared = false;
            this.isConsuming = false;

            if (this.receivingToken.IsCancellationRequested)
            {
                return;
            }

            // remove consumers with closed channel from active consumers
            await this.semaphore.ExecuteWithCondition(() =>
            {
                this.consumers.RemoveAll(c => c.Channel == eventArgs.RabbitChannel);
                return Task.FromResult(0);
            }, logger: this.logger, cancellationToken: this.receivingToken);

            if (eventArgs.ShutdownEventArgs.Initiator != ShutdownInitiator.Application)
            {
                if (StopOnChannelShutdown)
                {
                    this.logger.LogInformation("The receiving source is configured to be stopped on channel failure");
                    await InternalStop(OperationStopReason.Terminate, this.receivingToken);
                }
                else
                {
                    this.logger.LogInformation("The underlying channel has been closed, recovering the receiving source...");
                    await Recreate();
                }
            }
        }
        catch (Exception e)
        {
            if (e is OperationCanceledException && this.receivingToken.IsCancellationRequested)
            {
                return;
            }

            this.logger.LogError(e, "Failed to handle channel shutdown. Channel shutdown details: {ShutdownEventArgs}", eventArgs.ShutdownEventArgs);
        }
    }

    private async Task Recreate()
    {
        while (!this.receivingToken.IsCancellationRequested)
        {
            try
            {
                await Prepare(this.receivingToken);
                await StartConsuming(this.receivingToken);
                break;
            }
            catch (Exception e)
            {
                this.logger.LogWarning(e, "Failed to recover the receiving source. Retry in 1s");
                await Task.Delay(TimeSpan.FromSeconds(1), this.receivingToken);
            }
        }
    }

    /// <summary>
    /// Обработчик события о сбое обработки сообщения.
    /// </summary>
    /// <param name="delivery">
    /// Сообщение, обработка которого привела к сбою.
    /// </param>
    /// <param name="exception">
    /// Исключение сгенерированное во время сбоя.
    /// </param>
    private async Task OnFailure(RabbitDelivery delivery, Exception exception)
    {
        string label;
        if (delivery.IsResponse)
        {
            var args = delivery.Args;
            label = $"reply::{args.Exchange}::{args.RoutingKey}";
        }
        else
        {
            label = delivery.Label.ToString();
        }
        this.logger.LogWarning(exception, "Failed to process message labeled [{Label}] on queue [{ListeningSource}]", label, ListeningSource);

        if (ReceiverOptions.FailedDeliveryStrategy != null)
        {
            await ReceiverOptions.FailedDeliveryStrategy.Handle(new RabbitFailedConsumingContext(delivery, exception));
        }
    }

    /// <summary>
    /// Обрабатывает превышение времени ожидания ответа.
    /// </summary>
    /// <param name="correlationId">Корреляционный идентификатор, с помощью которого определяется принадлежность ответа определенному запросу.</param>
    private void OnResponseTimeout(string correlationId)
    {
        if (this.expectations.TryRemove(correlationId, out var expectation))
        {
            expectation.Timeout();
        }
    }

    /// <summary>
    /// Обработчик события о ненайденном обработчике сообщений.
    /// </summary>
    /// <param name="delivery">
    /// Сообщение, для которого не найден обработчик.
    /// </param>
    private async Task OnUnhandled(RabbitDelivery delivery)
    {
        this.logger.LogWarning("No handler for message labeled [{Label}] on queue [{ListeningSource}]. On connection string: [{BrokerUrl}]", delivery.Label, ListeningSource, BrokerUrl);

        if (ReceiverOptions.UnhandledDeliveryStrategy != null)
        {
            await ReceiverOptions.UnhandledDeliveryStrategy.Handle(new RabbitUnhandledConsumingContext(delivery));
        }
    }

    /// <summary>
    /// Пытается обработать сообщение как ответ.
    /// </summary>
    /// <param name="delivery">
    /// Входящее сообщение.
    /// </param>
    /// <returns>
    /// Если <c>true</c> - тогда сообщение обработано как ответ, иначе - <c>false</c>.
    /// </returns>
    private bool TryHandleAsResponse(RabbitDelivery delivery)
    {
        if (!delivery.IsResponse)
        {
            return false;
        }

        var correlationId = delivery.CorrelationId;

        if (!this.expectations.TryRemove(correlationId, out var expectation))
        {
            return false;
        }

        if (expectation.TimeoutTicket.HasValue)
        {
            this.ticketTimer.Cancel(expectation.TimeoutTicket.Value);
        }

        expectation.Complete(delivery);
        return true;
    }

    /// <summary>
    /// Пытается обработать сообщение как одностороннее.
    /// </summary>
    /// <param name="delivery">
    /// Входящее сообщение.
    /// </param>
    /// <returns>
    /// Если <c>true</c> - входящее сообщение обработано, иначе <c>false</c>.
    /// </returns>
    private async Task<bool> TryHandleAsSubscription(RabbitDelivery delivery)
    {
        if (this.consumingAction != null)
        {
            ReceiverOptions.IncomingMessageHeaderStorage.Store(delivery.Headers);
            await this.consumingAction(delivery);
            return true;
        }

        return false;
    }

    private record RabbitConsumer(AsyncEventingBasicConsumer Consumer, RabbitChannel Channel)
    {
        private static readonly ILogger Logger = LogManager.GetLogger<RabbitConsumer>();

        public bool IsConsuming { get; private set; }

        public string Tag { get; private set; }

        private AsyncEventHandler<BasicDeliverEventArgs> handler;

        public async Task StartConsuming(IListeningSource listeningSource, bool requiresAccept, AsyncEventHandler<BasicDeliverEventArgs> eventHandler, CancellationToken cancellationToken)
        {
            Consumer.ReceivedAsync += eventHandler;
            Tag = await Channel.StartConsuming(listeningSource, requiresAccept, Consumer, cancellationToken);
            this.handler = eventHandler;
            IsConsuming = true;
        }

        public async Task StopConsuming(CancellationToken token)
        {
            if (!IsConsuming)
            {
                return;
            }

            try
            {
                Consumer.ReceivedAsync -= this.handler;
                await Channel.StopConsuming(Tag, token);
                IsConsuming = false;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Failed to unsubscribe consumer");
            }
        }
    };
}
