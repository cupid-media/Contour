using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Sending;
using Contour.Sending.Exceptions;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Отправитель сообщений через один Confluent-продюсер.
///   В случае отправки запроса с ожиданием ответа, отправитель создает получателя ответного сообщения.
/// </summary>
internal sealed class KafkaProducer : IProducer
{
    private readonly ILogger logger;

    private readonly IKafkaProducerFactory producerFactory;

    private readonly KafkaBus bus;

    /// <summary>
    ///   Пересоздание продюсера выполняется единожды: параллельные отправки должны получить
    ///   один и тот же новый экземпляр, а не по своему.
    /// </summary>
    private readonly SemaphoreSlim rebuildSemaphore = new(1, 1);

    private IProducer<string, byte[]> producer;

    private volatile bool hasFatalError;

    private bool withCallback;

    public KafkaProducer(
        KafkaBus bus,
        string brokerUrl,
        IKafkaProducerFactory producerFactory,
        KafkaSenderOptions senderOptions)
    {
        Id = Guid.NewGuid();
        this.bus = bus;
        this.producerFactory = producerFactory;

        BrokerUrl = brokerUrl;
        Label = senderOptions.Label;
        SenderOptions = senderOptions;

        this.logger = LogManager.GetLogger($"{GetType().FullName}({BrokerUrl}, {Label}, {Id})");
    }

    public Guid Id { get; }

    public MessageLabel Label { get; }

    public string BrokerUrl { get; }

    public string ConnectionKey => BrokerUrl;

    /// <summary>
    ///   Получатель ответов: заводится вместе с продюсером, если маршрут объявлен как запрос-ответ.
    /// </summary>
    private KafkaReceiver CallbackReceiver { get; set; }

    private KafkaReceivingSource CallbackReceivingSource { get; set; }

    private KafkaSenderOptions SenderOptions { get; }

    /// <summary>
    ///   Продюсер исправен, пока создан и не получил фатальную ошибку от брокера.
    /// </summary>
    public bool IsInGoodCondition => this.producer != null && !this.hasFatalError;

    public async Task Prepare(CancellationToken token)
    {
        if (this.producer != null)
        {
            return;
        }

        this.logger.LogTrace("Building producer of [{Label}] at [{ConnectionString}]", Label, BrokerUrl);

        this.producer = this.producerFactory.Create(SenderOptions, OnProducerError);
        this.hasFatalError = false;

        if (this.withCallback)
        {
            await PrepareCallbackReceiver(token);
        }
    }

    public async Task Start(CancellationToken token)
    {
        if (CallbackReceivingSource != null)
        {
            await CallbackReceivingSource.StartConsuming(token);
        }
    }

    public async Task Stop(CancellationToken token)
    {
        if (CallbackReceivingSource != null)
        {
            await CallbackReceivingSource.StopConsuming(token);
        }

        Flush();
    }

    public void WithCallback()
    {
        this.withCallback = true;
    }

    /// <summary>
    ///   Пересобирает продюсера после фатальной ошибки: librdkafka помечает такой экземпляр
    ///   окончательно сломанным, и дальше он только отклоняет отправки.
    /// </summary>
    /// <remarks>
    ///   Вызывается отправителем перед выбором продюсера, а не из <see cref="Send{TRequest}"/>:
    ///   до отправки дело не дойдёт, потому что селектор не выдаёт продюсеров,
    ///   не прошедших <see cref="IsInGoodCondition"/>.
    /// </remarks>
    public async Task Recover()
    {
        if (!this.hasFatalError)
        {
            return;
        }

        await this.rebuildSemaphore.WaitAsync();
        try
        {
            if (!this.hasFatalError || this.producer == null)
            {
                // Пересобран, пока ждали семафор, либо продюсер уже освобождён.
                return;
            }

            this.logger.LogWarning("Rebuilding producer of [{Label}] at [{ConnectionString}] after a fatal broker error", Label, BrokerUrl);

            var broken = this.producer;

            // Флаг снимается до создания: ошибку, о которой сообщит уже новый продюсер,
            // затирать нельзя.
            this.hasFatalError = false;

            try
            {
                this.producer = this.producerFactory.Create(SenderOptions, OnProducerError);
            }
            catch (Exception ex)
            {
                this.hasFatalError = true;

                throw new FailedToSendMessageException($"Failed to rebuild producer of [{SenderOptions.Label}]", ex);
            }

            DisposeBroken(broken);
        }
        finally
        {
            this.rebuildSemaphore.Release();
        }
    }

    public async Task<EmitResult> Send<TRequest>(Message<TRequest> message)
        where TRequest : class
    {
        var current = this.producer;

        if (current == null)
        {
            throw new FailedToSendMessageException($"Producer of [{SenderOptions.Label}] is not prepared");
        }

        var topic = message.Label.Name;
        var kafkaMessage = new Message<string, byte[]>
        {
            Key = message.Key,
            Value = SenderOptions.PayloadConverter.FromObject(message.Payload),
            Headers = KafkaHeaderConverter.ToKafka(message.Headers)
        };

        if (!SenderOptions.ConfirmationRequired)
        {
            ProduceWithoutConfirmation(current, topic, kafkaMessage);
            return new EmitResult { UsedConnectionString = ConnectionKey };
        }

        await ProduceWithConfirmation(current, topic, kafkaMessage);

        return new EmitResult { UsedConnectionString = ConnectionKey };
    }

    public async Task<RequestResult<TResponse>> Request<TRequest, TResponse>(Message<TRequest> request)
        where TRequest : class
        where TResponse : class
    {
        if (CallbackReceivingSource == null)
        {
            throw new NotSupportedException(
                $"Sender of [{SenderOptions.Label}] cannot make requests: no reply topic is configured. " +
                "Call WithDefaultCallbackEndpoint() or WithCallbackTopic(topic) on the route.");
        }

        request.Headers[Headers.ReplyRoute] = CallbackReceivingSource.ReceiverOptions.Label.Name;
        var responseTimeout = Headers.Extract<TimeSpan?>(request.Headers, Headers.Timeout);
        var correlationId = (string)request.Headers[Headers.CorrelationId];
        var responseTask = CallbackReceivingSource.Expect<TResponse>(correlationId, responseTimeout);
        var sendResult = await Send(request);
        var response = await responseTask;

        return new RequestResult<TResponse>
        {
            Response = response.Payload,
            UsedConnectionString = sendResult.UsedConnectionString
        };
    }

    public async ValueTask DisposeAsync()
    {
        this.logger.LogTrace("Disposing producer of [{Label}]", Label);

        var callbackReceiver = CallbackReceiver;
        CallbackReceiver = null;
        CallbackReceivingSource = null;

        if (callbackReceiver != null)
        {
            await callbackReceiver.DisposeAsync();
        }

        await this.rebuildSemaphore.WaitAsync();
        try
        {
            var current = Interlocked.Exchange(ref this.producer, null);

            if (current == null)
            {
                return;
            }

            try
            {
                current.Flush(SenderOptions.FlushTimeout);
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error while flushing producer of [{Label}]", SenderOptions.Label);
            }
            finally
            {
                current.Dispose();
            }
        }
        finally
        {
            this.rebuildSemaphore.Release();
        }
    }

    private async Task PrepareCallbackReceiver(CancellationToken token)
    {
        CallbackReceiver = this.bus.RegisterReceiver(new KafkaReceiverOptions(SenderOptions.CallbackOptions)
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
    }

    /// <summary>
    ///   Освобождает сломанный продюсер. Буфер не дописывается: с фатальной ошибкой брокер
    ///   его уже не примет, и Flush только выждал бы весь свой таймаут.
    /// </summary>
    private void DisposeBroken(IProducer<string, byte[]> broken)
    {
        try
        {
            broken?.Dispose();
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Error while disposing the broken producer of [{Label}]", SenderOptions.Label);
        }
    }

    private void ProduceWithoutConfirmation(
        IProducer<string, byte[]> current,
        string topic,
        Message<string, byte[]> kafkaMessage)
    {
        try
        {
            current.Produce(topic, kafkaMessage);
        }
        catch (Exception ex) when (ex is ProduceException<string, byte[]> or ObjectDisposedException)
        {
            throw new FailedToSendMessageException($"Failed to enqueue a message to topic [{topic}]", ex);
        }
    }

    private async Task ProduceWithConfirmation(
        IProducer<string, byte[]> current,
        string topic,
        Message<string, byte[]> kafkaMessage)
    {
        // Produce с колбэком вместо ProduceAsync: продюсер не ждёт подтверждения на каждое
        // сообщение, батчинг librdkafka сохраняется, а отчёт о доставке всё равно дожидается.
        var confirmTcs = new TaskCompletionSource<DeliveryReport<string, byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);

        try
        {
            current.Produce(topic, kafkaMessage, deliveryReport => confirmTcs.TrySetResult(deliveryReport));
        }
        catch (Exception ex) when (ex is ProduceException<string, byte[]> or ObjectDisposedException)
        {
            throw new FailedToSendMessageException($"Failed to enqueue a message to topic [{topic}]", ex);
        }

        var delivered = SenderOptions.ConfirmationTimeout.HasValue
            ? await WaitWithTimeout(confirmTcs.Task, SenderOptions.ConfirmationTimeout.Value, topic)
            : await confirmTcs.Task;

        if (delivered.Error.IsError)
        {
            throw new FailedToSendMessageException(
                $"Failed to deliver a message to topic [{topic}]: {delivered.Error.Reason}");
        }

        this.logger.LogTrace(
            "Message emitted to topic [{Label}], partition [{Partition}], offset [{Offset}], status [{Status}]",
            delivered.Topic, delivered.Partition, delivered.Offset, delivered.Status);
    }

    private static async Task<DeliveryReport<string, byte[]>> WaitWithTimeout(
        Task<DeliveryReport<string, byte[]>> report,
        TimeSpan timeout,
        string topic)
    {
        try
        {
            return await report.WaitAsync(timeout);
        }
        catch (TimeoutException ex)
        {
            throw new FailedToSendMessageException(
                $"Delivery confirmation for topic [{topic}] was not received in {timeout}", ex);
        }
    }

    private void Flush()
    {
        var current = this.producer;

        if (current == null)
        {
            return;
        }

        try
        {
            current.Flush(SenderOptions.FlushTimeout);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Error while flushing producer of [{Label}]", SenderOptions.Label);
        }
    }

    private void OnProducerError(Error error)
    {
        if (error.IsFatal)
        {
            this.hasFatalError = true;
        }
    }
}
