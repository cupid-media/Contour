using System;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Serialization;

namespace Contour.Transport.Kafka.Configuration;

/// <summary>
///   Настройки получателя сообщений Kafka.
/// </summary>
/// <remarks>
///   Настройки Confluent-консьюмера не наследуются, а перечисляются здесь по одной и собираются
///   в <see cref="ConsumerConfig"/> методом <see cref="BuildConsumerConfig"/>: наследование
///   вытащило бы в публичный интерфейс библиотеки все две сотни свойств librdkafka,
///   из которых транспорту нужна дюжина.
/// </remarks>
public class KafkaReceiverOptions : IReceiverConfiguration
{
    public KafkaReceiverOptions()
    {
    }

    /// <summary>
    ///   Копирующий конструктор: переносит и настройки консьюмера, и расширения библиотеки.
    /// </summary>
    public KafkaReceiverOptions(KafkaReceiverOptions other)
    {
        Label = other.Label;
        ConnectionString = other.ConnectionString;
        GroupId = other.GroupId;
        AutoOffsetReset = other.AutoOffsetReset;
        EnableAutoCommit = other.EnableAutoCommit;
        PartitionAssignmentStrategy = other.PartitionAssignmentStrategy;
        QueuedMinMessages = other.QueuedMinMessages;
        MaxPartitionFetchBytes = other.MaxPartitionFetchBytes;
        SecurityProtocol = other.SecurityProtocol;
        SslCaPem = other.SslCaPem;
        SslCertificatePem = other.SslCertificatePem;
        SslKeyPem = other.SslKeyPem;
        EnableSslCertificateVerification = other.EnableSslCertificateVerification;
        Debug = other.Debug;

        ParallelismLevel = other.ParallelismLevel;
        PayloadConverter = other.PayloadConverter;
        CommitOffset = other.CommitOffset;
        CommitTimeout = other.CommitTimeout;
        MaxPendingAcknowledgements = other.MaxPendingAcknowledgements;
        ReceiverRegistration = other.ReceiverRegistration;
        FailedDeliveryStrategy = other.FailedDeliveryStrategy;
        UnhandledDeliveryStrategy = other.UnhandledDeliveryStrategy;
        AcceptRequired = other.AcceptRequired;
        Replies = other.Replies;
        ConnectionStringProvider = other.ConnectionStringProvider;
        IncomingMessageHeaderStorage = other.IncomingMessageHeaderStorage;
    }

    public MessageLabel Label { get; init; }

    public string ConnectionString { get; set; }

    public string GroupId { get; set; }

    public AutoOffsetReset? AutoOffsetReset { get; set; }

    public bool? EnableAutoCommit { get; set; }

    public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get; set; }

    /// <summary>
    ///   Сколько сообщений консьюмер держит в локальной очереди по каждой партиции.
    /// </summary>
    public int? QueuedMinMessages { get; set; }

    public int? MaxPartitionFetchBytes { get; set; }

    public SecurityProtocol? SecurityProtocol { get; set; }

    public string SslCaPem { get; set; }

    public string SslCertificatePem { get; set; }

    public string SslKeyPem { get; set; }

    public bool? EnableSslCertificateVerification { get; set; }

    public string Debug { get; set; }

    public uint? ParallelismLevel { get; set; }

    /// <summary>
    ///   Провайдер адресов брокеров: если задан, его значение важнее заданного явно.
    /// </summary>
    public IConnectionStringProvider ConnectionStringProvider { get; set; }

    public IPayloadConverter PayloadConverter { get; set; }

    /// <summary>
    ///   Хранилище заголовков входящего сообщения: позволяет переносить сквозные заголовки
    ///   (<c>x-correlation-id</c>, <c>x-breadcrumbs</c>) в сообщения, отправляемые при обработке.
    /// </summary>
    public IIncomingMessageHeaderStorage IncomingMessageHeaderStorage { get; set; }

    public int CommitOffset { get; set; }

    public TimeSpan CommitTimeout { get; set; }

    /// <summary>
    ///   Сколько прочитанных сообщений может одновременно ждать отложенного решения обработчика
    ///   (например, накопительного). Достигнув предела, воркер приостанавливает чтение партиций,
    ///   пока решения не будут приняты, — так прочитанное не копится в памяти без ограничений.
    ///   Предел действует на каждый воркер отдельно (см. <see cref="ParallelismLevel"/>).
    /// </summary>
    public int MaxPendingAcknowledgements { get; set; }

    /// <summary>
    ///   Замыкание регистрации обработчика: вызывает <c>RegisterConsumer&lt;T&gt;</c> получателя,
    ///   сохраняя тип сообщения, известный на момент конфигурирования.
    /// </summary>
    public Action<IReceiver> ReceiverRegistration { get; set; }

    /// <summary>
    ///   Что делать с сообщением, обработка которого завершилась исключением.
    ///   <c>null</c> — только записать ошибку в журнал и пропустить сообщение.
    /// </summary>
    public IFailedDeliveryStrategy FailedDeliveryStrategy { get; set; }

    /// <summary>
    ///   Что делать с сообщением, для которого не нашлось обработчика.
    ///   <c>null</c> — только записать предупреждение в журнал и пропустить сообщение.
    /// </summary>
    public IUnhandledDeliveryStrategy UnhandledDeliveryStrategy { get; set; }

    /// <summary>
    ///   Требуется ли явное подтверждение обработки через
    ///   <see cref="Contour.Receiving.IConsumingContext{T}.AcceptAsync"/>. Без него оффсет сообщения
    ///   не продвигается, даже если обработчик завершился успешно.
    /// </summary>
    public bool AcceptRequired { get; set; }

    /// <summary>
    ///   Обработчик отвечает на запросы (<see cref="Contour.Receiving.IConsumingContext{T}.ReplyAsync{TResponse}"/>)
    ///   или пересылает сообщения (<c>ForwardAsync</c>). Тогда для кластера получателя заводится продюсер.
    /// </summary>
    public bool Replies { get; set; }

    /// <summary>
    ///   Собирает конфигурацию Confluent-консьюмера.
    /// </summary>
    internal ConsumerConfig BuildConsumerConfig()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = ConnectionString,
            GroupId = GroupId
        };

        if (AutoOffsetReset.HasValue)
        {
            config.AutoOffsetReset = AutoOffsetReset.Value;
        }

        if (EnableAutoCommit.HasValue)
        {
            config.EnableAutoCommit = EnableAutoCommit.Value;
        }

        if (EnableAutoCommit == true)
        {
            // Иначе клиент запоминал бы и автокоммитил оффсет полученного, но ещё не обработанного
            // сообщения. Безопасные позиции трекера ему отдаются явно (StoreOffset).
            config.EnableAutoOffsetStore = false;
        }

        if (PartitionAssignmentStrategy.HasValue)
        {
            config.PartitionAssignmentStrategy = PartitionAssignmentStrategy.Value;
        }

        if (QueuedMinMessages.HasValue)
        {
            config.QueuedMinMessages = QueuedMinMessages.Value;
        }

        if (MaxPartitionFetchBytes.HasValue)
        {
            config.MaxPartitionFetchBytes = MaxPartitionFetchBytes.Value;
        }

        if (SecurityProtocol.HasValue)
        {
            config.SecurityProtocol = SecurityProtocol.Value;
        }

        if (SslCaPem != null)
        {
            config.SslCaPem = SslCaPem;
        }

        if (SslCertificatePem != null)
        {
            config.SslCertificatePem = SslCertificatePem;
        }

        if (SslKeyPem != null)
        {
            config.SslKeyPem = SslKeyPem;
        }

        if (EnableSslCertificateVerification.HasValue)
        {
            config.EnableSslCertificateVerification = EnableSslCertificateVerification.Value;
        }

        if (Debug != null)
        {
            config.Debug = Debug;
        }

        return config;
    }

    /// <summary>
    ///   Снимок опций для конкретного топика: у каждого ресивера свой независимый экземпляр,
    ///   общие дефолты шины на него больше не влияют.
    /// </summary>
    internal KafkaReceiverOptions CloneFor(string label)
    {
        return new KafkaReceiverOptions(this) { Label = MessageLabel.From(label) };
    }
}
