using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Helpers;
using Contour.Helpers.Timing;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal sealed class KafkaReceivingSource : IReceivingSource
{
    private readonly ConcurrentDictionary<string, MessageExpectation> expectations = new();
    private readonly object expectationsLock = new();
    private readonly SemaphoreSlim semaphore = new(1, 1);
    private readonly ILogger logger;
    private readonly KafkaBus bus;
    private readonly IKafkaConsumerFactory consumerFactory;
    private readonly List<KafkaConsumer> consumers = [];

    private ConsumingAction consumingAction;

    private bool isPrepared;
    private volatile bool isConsuming;

    /// <summary>
    ///   Завершается, когда обработчик вернул управление. Результат — решение о продвижении
    ///   оффсета, которое может прийти позже (см. <see cref="KafkaMessageHandler"/>).
    /// </summary>
    private delegate Task<ConsumingResult> ConsumingAction(KafkaDelivery delivery, CancellationToken stopToken);

    private ITicketTimer ticketTimer;

    public KafkaReceivingSource(
        KafkaBus bus,
        string brokerUrl,
        IKafkaConsumerFactory consumerFactory,
        KafkaReceiverOptions receiverOptions)
    {
        Id = Guid.NewGuid();
        this.bus = bus;
        this.consumerFactory = consumerFactory;

        ReceiverOptions = receiverOptions;
        BrokerUrl = brokerUrl;

        this.logger = LogManager.GetLogger($"{GetType().FullName}({BrokerUrl}, {ReceiverOptions.Label}, {Id})");
    }

    public Guid Id { get; }

    public event Action<IReceivingSource> OnStartConsuming;

    public event Action<IReceivingSource> OnFinishConsuming;

    public KafkaReceiverOptions ReceiverOptions { get; }

    public string BrokerUrl { get; }

    public bool IsHealthy => this.isConsuming && this.consumers.All(c => c.IsAlive);

    public KafkaDelivery BuildDeliveryFrom(ConsumeResult<Ignore, byte[]> consumeResult)
    {
        return new KafkaDelivery(
            this.bus,
            BrokerUrl,
            consumeResult.Topic,
            consumeResult.Message.Value,
            KafkaHeaderConverter.FromKafka(consumeResult.Message.Headers),
            ReceiverOptions.PayloadConverter);
    }

    public async Task<Message<TResponse>> Expect<TResponse>(string correlationId, TimeSpan? timeout) where TResponse : class
    {
        if (this.expectations.TryGetValue(correlationId, out var expectation))
        {
            return (Message<TResponse>)await expectation.Task;
        }

        // Используется блокировка, чтобы гарантировать, что метод CreateExpectation будет вызван один раз, т.к. это не гарантируется реализацией метода GetOrAdd.
        lock (this.expectationsLock)
        {
            expectation = this.expectations.GetOrAdd(correlationId, c => CreateExpectation<TResponse>(c, timeout));
        }

        return (Message<TResponse>)await expectation.Task;
    }

    public void RegisterConsumer<T>(IConsumer<T> consumer) where T : class
    {
        if (consumer is not IConsumerOf<T> consumerOf)
        {
            throw new BusConfigurationException(
                $"Consumer of [{typeof(T).Name}] registered for topic [{ReceiverOptions.Label}] must implement " +
                $"IConsumerOf<{typeof(T).Name}>, but was [{consumer?.GetType().Name ?? "null"}].");
        }

        this.consumingAction = async (delivery, stopToken) =>
        {
            IConsumingContext<T> context = new DefaultConsumingContext<T>(this.bus, delivery.UnpackAs<T>(), delivery);

            await consumerOf.HandleAsync(context);

            return ResolveConsumingResult(delivery, stopToken);
        };
    }

    public async Task Prepare(CancellationToken cancellationToken)
    {
        await this.semaphore.ExecuteWithCondition(() =>
        {
            this.logger.LogDebug("Prepare consuming on [{Topic}]", ReceiverOptions.Label);
            this.ticketTimer ??= new RoughTicketTimer(TimeSpan.FromMilliseconds(100));

            var count = (int)(ReceiverOptions.ParallelismLevel ?? 1);

            for (var i = this.consumers.Count; i < count; i++)
            {
                this.consumers.Add(new KafkaConsumer(ReceiverOptions, this.consumerFactory));
            }

            this.isPrepared = true;
            return Task.FromResult(0);
        }, () => !this.isPrepared, logger: this.logger, cancellationToken: cancellationToken);
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
            this.logger.LogDebug("Starting consuming on [{Topic}]", ReceiverOptions.Label);

            try
            {
                await Task.WhenAll(this.consumers.Select(async c =>
                {
                    try
                    {
                        await c.StartConsuming(OnMessage, cancellationToken);
                    }
                    catch (Exception e)
                    {
                        this.logger.LogError(e, "Failed to start consumer of topic [{Topic}]", ReceiverOptions.Label);
                        throw;
                    }
                }));
            }
            catch
            {
                // Не сумевший подписаться воркер уже завершён; остальных сворачиваем, чтобы за
                // неудачным стартом не остались живые консьюмеры, которых некому остановить.
                await Task.WhenAll(this.consumers.Select(c => c.StopConsuming(CancellationToken.None)));
                throw;
            }

            this.isConsuming = true;
            this.logger.LogInformation("Receiving source of topic {Label} started with {WorkersCount} worker(s)", ReceiverOptions.Label, this.consumers.Count);
            return 0;
        }, () => !this.isConsuming && this.isPrepared, logger: this.logger, cancellationToken: cancellationToken);
    }

    public async Task StopConsuming(CancellationToken cancellationToken)
    {
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            this.isConsuming = false;
            this.isPrepared = false;
            this.logger.LogInformation("Stopping consuming on [{Topic}]: waiting for {WorkersCount} worker(s) to finish current messages", ReceiverOptions.Label, this.consumers.Count);

            // Ожидающие запросы снимаются сразу: без топика ответов они всё равно не дождутся.
            this.expectations.Values.ForEach(e => e.Cancel());
            this.expectations.Clear();

            await Task.WhenAll(this.consumers.Select(async c =>
            {
                try
                {
                    await c.StopConsuming(cancellationToken);
                }
                catch (Exception e)
                {
                    this.logger.LogError(e, "Failed to stop consumer of topic [{Topic}]", ReceiverOptions.Label);
                    throw;
                }
            }));

            this.logger.LogTrace("ReceivingSource stopped successfully");
            return 0;
        }, () => this.isConsuming || this.isPrepared, logger: this.logger, cancellationToken: cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await StopConsuming(CancellationToken.None);
        this.ticketTimer?.Dispose();
    }

    private async Task<ConsumingResult> OnMessage(ConsumeResult<Ignore, byte[]> consumeResult, CancellationToken stopToken)
    {
        OnStartConsuming?.Invoke(this);
        try
        {
            return await Deliver(BuildDeliveryFrom(consumeResult), stopToken);
        }
        finally
        {
            OnFinishConsuming?.Invoke(this);
        }
    }

    private async Task<ConsumingResult> Deliver(KafkaDelivery delivery, CancellationToken stopToken)
    {
        DiagnosticProps.Store(DiagnosticProps.Names.ConsumerConnectionString, BrokerUrl);
        using var _ = this.logger.BeginScope(("ConsumerConnectionString", BrokerUrl));

        IDisposable originalMsgIdScope = null;
        IDisposable breadcrumbsScope = null;
        try
        {
            this.logger.LogTrace("Received delivery labeled [{Label}]", delivery.Label);

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
                if (this.consumingAction != null)
                {
                    return await HandleAsSubscription(delivery, stopToken);
                }

                if (TryHandleAsResponse(delivery))
                {
                    return ConsumingResult.Acknowledged;
                }

                return await OnUnhandled(delivery);
            }
            catch (Exception ex)
            {
                return await OnFailure(delivery, ex);
            }
            finally
            {
                stopwatch.Stop();
                this.logger.LogTrace("Message labeled [{Label}] processed in {ElapsedMilliseconds} ms", delivery.Label, stopwatch.ElapsedMilliseconds);
            }
        }
        finally
        {
            originalMsgIdScope?.Dispose();
            breadcrumbsScope?.Dispose();
        }
    }

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

        return new MessageExpectation(delivery => delivery.UnpackAs<TResponse>(), timeoutTicket);
    }

    private ConsumingResult ResolveConsumingResult(KafkaDelivery delivery, CancellationToken stopToken)
    {
        if (delivery.Acknowledged is { } acknowledged)
        {
            return ConsumingResult.FromResult(acknowledged);
        }

        if (!ReceiverOptions.AcceptRequired)
        {
            return ConsumingResult.Acknowledged;
        }

        return delivery.WhenAcknowledged(stopToken);
    }

    private async Task<ConsumingResult> OnFailure(KafkaDelivery delivery, Exception exception)
    {
        var strategy = ReceiverOptions.FailedDeliveryStrategy;

        if (strategy == null)
        {
            // Без стратегии упавшее сообщение считается обработанным: удерживать оффсет
            // значило бы навсегда остановить продвижение партиции на нём.
            this.logger.LogError(
                exception,
                "Handling of a message from topic [{Label}] has failed and no failed delivery strategy is configured, the message is skipped",
                ReceiverOptions.Label);

            return ConsumingResult.Acknowledged;
        }

        this.logger.LogError(
            exception,
            "Handling of a message from topic [{Label}] has failed, applying the failed delivery strategy",
            ReceiverOptions.Label);

        await strategy.Handle(new KafkaFailedConsumingContext(delivery, exception));

        // Стратегия может удержать оффсет через RejectAsync(requeue: true); иначе сообщение
        // считается разобранным — двигаться дальше можно.
        return ConsumingResult.FromResult(delivery.Acknowledged ?? true);
    }

    private void OnResponseTimeout(string correlationId)
    {
        if (this.expectations.TryRemove(correlationId, out var expectation))
        {
            this.logger.LogWarning("Response with correlationId [{CorrelationId}] was not received in time", correlationId);
            expectation.Timeout();
        }
    }

    private async Task<ConsumingResult> OnUnhandled(KafkaDelivery delivery)
    {
        var strategy = ReceiverOptions.UnhandledDeliveryStrategy;

        if (strategy == null)
        {
            this.logger.LogWarning("No handler for a message from topic [{Label}], the message is skipped", delivery.Label);

            return ConsumingResult.Acknowledged;
        }

        this.logger.LogWarning("No handler for a message from topic [{Label}], applying the unhandled delivery strategy", delivery.Label);

        await strategy.Handle(new KafkaUnhandledConsumingContext(delivery));

        return ConsumingResult.FromResult(delivery.Acknowledged ?? true);
    }

    private bool TryHandleAsResponse(KafkaDelivery delivery)
    {
        var correlationId = delivery.CorrelationId;

        if (string.IsNullOrEmpty(correlationId))
        {
            return false;
        }

        if (!this.expectations.TryRemove(correlationId, out var expectation))
        {
            this.logger.LogTrace(
                "Response with correlationId [{CorrelationId}] from topic [{Label}] is not awaited here and was skipped",
                correlationId, delivery.Label);

            return true;
        }

        if (expectation.TimeoutTicket.HasValue)
        {
            this.ticketTimer.Cancel(expectation.TimeoutTicket.Value);
        }

        expectation.Complete(delivery);
        return true;
    }

    private Task<ConsumingResult> HandleAsSubscription(KafkaDelivery delivery, CancellationToken stopToken)
    {
        ReceiverOptions.IncomingMessageHeaderStorage?.Store(delivery.Headers);

        return this.consumingAction(delivery, stopToken);
    }
}
