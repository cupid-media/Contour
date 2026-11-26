using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Serialization;
using Contour.Transport.Kafka.Internal;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Configuration;

public class KafkaBusConfigurator
{
    private static readonly ILogger Logger = LogManager.GetLogger<KafkaBusConfigurator>();

    private readonly KafkaReceiverOptions receiverDefaults;

    private readonly KafkaSenderOptions senderDefaults;

    private Endpoint endpoint;

    private IReadOnlyCollection<string> excludedHeaders;

    internal KafkaBusConfigurator()
    {
        this.endpoint = KafkaBusDefaults.DefaultEndpoint;
        this.receiverDefaults = KafkaBusDefaults.DefaultReceiverOptions;
        this.senderDefaults = KafkaBusDefaults.DefaultSenderOptions;
        PayloadConverter = new JsonPayloadConverter();
    }

    public bool AutoStart { get; set; } = true;

    public IPayloadConverter PayloadConverter { get; private set; }

    public IBusLifecycleHandler BusLifecycleHandler { get; private set; }

    public List<KafkaReceiverConfigurator> ReceiverConfigurators { get; } = [];

    private MessageLabelResolver MessageLabelResolver { get; } = new();

    public List<KafkaSenderConfigurator> SenderConfigurators { get; } = [];

    /// <summary>
    ///   Фабрика низкоуровневых консьюмеров: подменяется в тестах.
    /// </summary>
    internal IKafkaConsumerFactory ConsumerFactory { get; set; } = new ConfluentKafkaConsumerFactory();

    /// <summary>
    ///   Фабрика низкоуровневых продюсеров: подменяется в тестах.
    /// </summary>
    internal IKafkaProducerFactory ProducerFactory { get; set; } = new ConfluentKafkaProducerFactory();

    /// <summary>
    ///   Имя конечной точки: попадает в заголовок <c>x-breadcrumbs</c> отправляемых сообщений.
    /// </summary>
    public KafkaBusConfigurator WithEndpointName(string name)
    {
        this.endpoint = new Endpoint(name);
        return this;
    }

    /// <summary>
    ///   Список брокеров для всех получателей и отправителей, объявленных далее.
    /// </summary>
    public KafkaBusConfigurator WithConnectionString(string connectionString)
    {
        this.receiverDefaults.ConnectionString = connectionString;
        this.senderDefaults.ConnectionString = connectionString;
        return this;
    }

    public KafkaBusConfigurator WithGroupId(string groupId)
    {
        this.receiverDefaults.GroupId = groupId;
        return this;
    }

    public KafkaBusConfigurator WithPayloadConverter(IPayloadConverter converter)
    {
        PayloadConverter = converter;
        return this;
    }

    /// <summary>
    ///   Время ожидания ответа на запрос по умолчанию для всех маршрутов, объявленных далее.
    /// </summary>
    public KafkaBusConfigurator WithRequestTimeout(TimeSpan? requestTimeout)
    {
        if (requestTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(requestTimeout), "Request timeout must be positive");
        }

        this.senderDefaults.RequestTimeout = requestTimeout;
        return this;
    }

    /// <summary>
    ///   Регистрирует стратегию обработки сообщений, обработка которых завершилась провалом.
    /// </summary>
    public KafkaBusConfigurator OnFailed(IFailedDeliveryStrategy failedDeliveryStrategy)
    {
        this.receiverDefaults.FailedDeliveryStrategy = failedDeliveryStrategy;
        return this;
    }

    /// <summary>
    ///   Регистрирует обработчик сообщений, обработка которых завершилась провалом.
    /// </summary>
    public KafkaBusConfigurator OnFailed(Func<IFailedConsumingContext, Task> failedDeliveryHandler)
    {
        return OnFailed(new LambdaFailedDeliveryStrategy(failedDeliveryHandler));
    }

    /// <summary>
    ///   Регистрирует стратегию обработки сообщений, для которых не нашлось обработчика.
    /// </summary>
    public KafkaBusConfigurator OnUnhandled(IUnhandledDeliveryStrategy unhandledDeliveryStrategy)
    {
        this.receiverDefaults.UnhandledDeliveryStrategy = unhandledDeliveryStrategy;
        return this;
    }

    /// <summary>
    ///   Регистрирует обработчик сообщений, для которых не нашлось обработчика.
    /// </summary>
    public KafkaBusConfigurator OnUnhandled(Func<IUnhandledConsumingContext, Task> unhandledDeliveryHandler)
    {
        return OnUnhandled(new LambdaUnhandledDeliveryStrategy(unhandledDeliveryHandler));
    }

    /// <summary>
    ///   Включает перекладывание упавших сообщений в fault-топик — так же, как это делает
    ///   RabbitMQ-транспорт со своей fault-очередью.
    /// </summary>
    public KafkaBusConfigurator UseDefaultFaultStrategy()
    {
        return OnFailed(new KafkaDefaultFaultStrategy());
    }

    public KafkaBusConfigurator WithBusLifecycleHandler(IBusLifecycleHandler handler)
    {
        BusLifecycleHandler = handler;
        return this;
    }

    /// <summary>
    ///   Заголовки входящих сообщений, которые не переносятся в исходящие.
    /// </summary>
    public KafkaBusConfigurator WithExcludedHeaders(IReadOnlyCollection<string> excludedHeaders)
    {
        this.excludedHeaders = excludedHeaders;
        return this;
    }

    public KafkaBusConfigurator WithIncomingMessageHeaderStorage(IIncomingMessageHeaderStorage storage)
    {
        this.receiverDefaults.IncomingMessageHeaderStorage = storage;
        this.senderDefaults.IncomingMessageHeaderStorage = storage;
        return this;
    }

    public KafkaBusConfigurator WithParallelismLevel(uint parallelismLevel)
    {
        if (parallelismLevel == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(parallelismLevel), "Parallelism level must be greater than zero");
        }

        this.receiverDefaults.ParallelismLevel = parallelismLevel;
        return this;
    }

    /// <summary>
    ///   Коммит-чекпоинт: фиксировать позиции каждые <paramref name="commitOffset"/> обработанных сообщений.
    /// </summary>
    public KafkaBusConfigurator WithCommitOffset(int commitOffset)
    {
        if (commitOffset <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(commitOffset), "Commit offset must be positive");
        }

        this.receiverDefaults.CommitOffset = commitOffset;
        return this;
    }

    /// <summary>
    ///   Максимальное время, которое обработанный хвост тихой партиции может оставаться незакоммиченным.
    /// </summary>
    public KafkaBusConfigurator WithCommitTimeout(TimeSpan commitTimeout)
    {
        if (commitTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(commitTimeout), "Commit timeout must be positive");
        }

        this.receiverDefaults.CommitTimeout = commitTimeout;
        return this;
    }

    /// <summary>
    ///   Сколько прочитанных сообщений может одновременно ждать отложенного решения обработчика;
    ///   по достижении предела чтение приостанавливается.
    /// </summary>
    public KafkaBusConfigurator WithMaxPendingAcknowledgements(int maxPendingAcknowledgements)
    {
        if (maxPendingAcknowledgements <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPendingAcknowledgements), "Max pending acknowledgements must be positive");
        }

        this.receiverDefaults.MaxPendingAcknowledgements = maxPendingAcknowledgements;
        return this;
    }

    /// <summary>
    ///   Сколько раз повторять отправку сообщения при ошибке доставки.
    /// </summary>
    public KafkaBusConfigurator WithFailoverAttempts(int failoverAttempts)
    {
        if (failoverAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(failoverAttempts), "Failover attempts must be positive");
        }

        this.senderDefaults.FailoverAttempts = failoverAttempts;
        return this;
    }

    /// <summary>
    ///   Дожидаться ли отчёта о доставке при отправке.
    /// </summary>
    public KafkaBusConfigurator WithConfirmation(bool required = true, TimeSpan? timeout = null)
    {
        this.senderDefaults.ConfirmationRequired = required;

        if (timeout.HasValue)
        {
            if (timeout.Value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeout), "Confirmation timeout must be positive");
            }

            this.senderDefaults.ConfirmationTimeout = timeout;
        }

        return this;
    }

    /// <summary>
    ///   Сколько сообщений консьюмер держит в локальной очереди по каждой партиции и сколько
    ///   байт забирает из неё за раз — ближайший аналог prefetch у RabbitMQ.
    /// </summary>
    /// <remarks>
    ///   Именно <c>queued.min.messages</c>, а не <c>max.poll.records</c> из Java-клиента:
    ///   librdkafka такого свойства не знает и отвергает конфигурацию целиком, а
    ///   <see cref="Confluent.Kafka.IConsumer{TKey,TValue}.Consume(TimeSpan)"/> и так отдаёт
    ///   по одному сообщению за вызов.
    /// </remarks>
    public KafkaBusConfigurator WithFetch(int queuedMinMessages, int? maxPartitionFetchBytes = null)
    {
        if (queuedMinMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(queuedMinMessages), "Queued min messages must be positive");
        }

        this.receiverDefaults.QueuedMinMessages = queuedMinMessages;

        if (maxPartitionFetchBytes.HasValue)
        {
            if (maxPartitionFetchBytes.Value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxPartitionFetchBytes), "Max partition fetch bytes must be positive");
            }

            this.receiverDefaults.MaxPartitionFetchBytes = maxPartitionFetchBytes.Value;
        }

        return this;
    }

    /// <summary>
    ///   Провайдер адресов брокеров, вычисляемых в рантайме: применяется к получателям
    ///   и маршрутам, объявленным далее.
    /// </summary>
    public KafkaBusConfigurator WithBootstrapServersProvider(IConnectionStringProvider provider)
    {
        this.receiverDefaults.ConnectionStringProvider = provider;
        this.senderDefaults.BootstrapServersProvider = provider;
        return this;
    }

    public KafkaBusConfigurator WithAutoOffsetReset(AutoOffsetReset autoOffsetReset)
    {
        this.receiverDefaults.AutoOffsetReset = autoOffsetReset;
        return this;
    }

    public KafkaBusConfigurator WithAutoCommit(bool enableAutoCommit = true)
    {
        this.receiverDefaults.EnableAutoCommit = enableAutoCommit;
        return this;
    }

    public KafkaBusConfigurator WithPartitionAssignmentStrategy(PartitionAssignmentStrategy strategy)
    {
        this.receiverDefaults.PartitionAssignmentStrategy = strategy;
        return this;
    }

    public KafkaBusConfigurator WithSecurityProtocol(SecurityProtocol protocol)
    {
        this.receiverDefaults.SecurityProtocol = protocol;
        this.senderDefaults.SecurityProtocol = protocol;
        return this;
    }

    public KafkaBusConfigurator WithSslCaPem(string sslCaPem)
    {
        this.receiverDefaults.SslCaPem = sslCaPem;
        this.senderDefaults.SslCaPem = sslCaPem;
        return this;
    }

    public KafkaBusConfigurator WithSslCertificatePem(string sslCertificatePem)
    {
        this.receiverDefaults.SslCertificatePem = sslCertificatePem;
        this.senderDefaults.SslCertificatePem = sslCertificatePem;
        return this;
    }

    public KafkaBusConfigurator WithSslKeyPem(string sslKeyPem)
    {
        this.receiverDefaults.SslKeyPem = sslKeyPem;
        this.senderDefaults.SslKeyPem = sslKeyPem;
        return this;
    }

    public KafkaBusConfigurator WithSslCertificateVerification(bool enableSslCertificateVerification = true)
    {
        this.receiverDefaults.EnableSslCertificateVerification = enableSslCertificateVerification;
        this.senderDefaults.EnableSslCertificateVerification = enableSslCertificateVerification;
        return this;
    }

    public KafkaBusConfigurator WithDebug(string debug)
    {
        this.receiverDefaults.Debug = debug;
        this.senderDefaults.Debug = debug;
        return this;
    }

    /// <summary>
    ///   Объявляет получателя сообщений топика. Получатель забирает снимок текущих
    ///   дефолтов шины, поэтому последующие <c>With*</c> на конфигураторе шины его не меняют.
    /// </summary>
    /// <summary>
    ///   Объявляет получателя, беря имя топика из атрибута <see cref="MessageAttribute"/>
    ///   на типе сообщения.
    /// </summary>
    public KafkaReceiverConfigurator On<TModel>()
        where TModel : class
    {
        return On(MessageLabelResolver.ResolveFrom<TModel>().Name);
    }

    public KafkaReceiverConfigurator On(string topic)
    {
        KafkaTopic.Validated(topic, nameof(topic));

        if (HasRegisteredConsumerFor(topic))
        {
            throw new ArgumentException($"Receiver for topic [{topic}] is already registered", nameof(topic));
        }

        var configurator = new KafkaReceiverConfigurator(topic, this.receiverDefaults);
        ReceiverConfigurators.Add(configurator);
        return configurator;
    }

    /// <summary>
    ///   Объявляет маршрут отправки: метка сообщений совпадает с именем топика.
    /// </summary>
    public KafkaSenderConfigurator Route(string topic)
    {
        return Route(MessageLabel.From(KafkaTopic.Validated(topic, nameof(topic))));
    }

    /// <summary>
    ///   Объявляет маршрут отправки, беря имя топика из атрибута <see cref="MessageAttribute"/>
    ///   на типе сообщения.
    /// </summary>
    public KafkaSenderConfigurator Route<T>()
        where T : class
    {
        return Route(MessageLabelResolver.ResolveFrom<T>().Name);
    }

    public KafkaSenderConfigurator Route(MessageLabel label)
    {
        if (HasRegisteredProducerFor(label))
        {
            throw new ArgumentException($"Sender for label [{label}] is already registered", nameof(label));
        }

        var configurator = new KafkaSenderConfigurator(label, this.senderDefaults);
        SenderConfigurators.Add(configurator);
        return configurator;
    }

    internal void PostConfigure()
    {
        DeclareFaultRoute();

        // Конвертер задаётся на уровне шины и раздаётся компонентам, у которых он не переопределён,
        // — независимо от того, когда он был выставлен относительно объявления топиков.
        foreach (var receiverConfigurator in ReceiverConfigurators)
        {
            receiverConfigurator.Options.PayloadConverter ??= PayloadConverter;

            // Консьюмер строится один раз, поэтому адрес из провайдера разрешается здесь.
            var provider = receiverConfigurator.Options.ConnectionStringProvider;

            if (provider != null)
            {
                receiverConfigurator.Options.ConnectionString = provider.GetConnectionString(receiverConfigurator.Options.Label);
            }

            if (this.excludedHeaders != null)
            {
                receiverConfigurator.Options.IncomingMessageHeaderStorage?.RegisterExcludedHeaders(this.excludedHeaders);
            }
        }

        foreach (var senderConfigurator in SenderConfigurators)
        {
            senderConfigurator.Options.PayloadConverter ??= PayloadConverter;
        }
    }

    internal IBus Create()
    {
        ResolveCallbackTopics();

        return new KafkaBus(new KafkaBusConfiguration
        {
            Endpoint = this.endpoint,
            LifecycleHandler = BusLifecycleHandler,
            Serializer = PayloadConverter,
            ConnectionString = this.receiverDefaults.ConnectionString ?? this.senderDefaults.ConnectionString,
            ReceiverDefaults = this.receiverDefaults,
            SenderDefaults = this.senderDefaults,
            ReceiverConfigurations = ReceiverConfigurators.Select(c => c.Options).ToList(),
            SenderConfigurations = SenderConfigurators.Select(c => c.Options).ToList(),
            ConsumerFactory = ConsumerFactory,
            ProducerFactory = ProducerFactory
        });
    }

    public virtual void Validate()
    {
        Logger.LogTrace(
            "Вызван метод для валидации конфигурации. Адрес конечной точки - [{EndpointAddress}], получаемые сообщения - [{ReceiverTopics}], отправляемые сообщения - [{SenderLabels}]",
            this.endpoint.Address,
            string.Join(";", ReceiverConfigurators.Select(c => c.Topic)),
            string.Join(";", SenderConfigurators.Select(c => c.Label)));

        if (PayloadConverter == null)
        {
            throw new BusConfigurationException("PayloadConverter is not set.");
        }

        if (this.endpoint.Address == null)
        {
            throw new BusConfigurationException("Не задано название компонента (Endpoint).");
        }

        if (ReceiverConfigurators.Count == 0 && SenderConfigurators.Count == 0)
        {
            throw new BusConfigurationException(
                "No senders and receivers are registered. Call On(topic).ReactWith(consumer) or Route(topic) before starting the bus.");
        }

        SenderConfigurators.ForEach(c => c.Validate());
        ReceiverConfigurators.ForEach(c => c.Validate());
    }

    /// <summary>
    ///   Проставляет каждому маршруту запрос-ответа топик, в котором он ждёт ответы:
    ///   у каждой метки он свой, поэтому чужие ответы не приходится вычитывать и отбрасывать.
    /// </summary>
    private void ResolveCallbackTopics()
    {
        foreach (var configurator in SenderConfigurators)
        {
            var options = configurator.Options;

            if (!options.RequiresCallback)
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(options.CallbackTopic))
            {
                options.CallbackTopic = ResolveReplyTopic(options.Label);
            }

            options.CallbackOptions = KafkaBusDefaults.CallbackReceiverOptions(
                this.receiverDefaults,
                options.CallbackTopic,
                this.endpoint.Address);

            options.CallbackOptions.PayloadConverter ??= PayloadConverter;
        }
    }

    /// <summary>
    ///   Топик ответов маршрута: заданный на шине явно либо производный от имени конечной
    ///   точки и метки маршрута.
    /// </summary>
    private string ResolveReplyTopic(MessageLabel label)
    {
        // У маршрута на любую метку своей метки нет, и топик ответов у него один на конечную точку.
        return label.IsAny
            ? KafkaTopic.DerivedFrom(this.endpoint.Address, "reply")
            : KafkaTopic.DerivedFrom(this.endpoint.Address, $"{label.Name}..reply");
    }

    private void DeclareFaultRoute()
    {
        var faultTopic = KafkaTopic.FaultOf(this.endpoint.Address);

        if (ReceiverConfigurators.Any(c => c.Options.FailedDeliveryStrategy is KafkaDefaultFaultStrategy)
            && !HasRegisteredProducerFor(faultTopic))
        {
            Route(faultTopic);
        }
    }

    private bool HasRegisteredConsumerFor(string topic)
    {
        return ReceiverConfigurators.Any(c => topic.Equals(c.Topic));
    }

    private bool HasRegisteredProducerFor(MessageLabel label)
    {
        return SenderConfigurators.Any(c => c.Label.Equals(label));
    }
}
