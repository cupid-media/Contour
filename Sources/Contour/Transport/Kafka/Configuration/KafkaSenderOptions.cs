using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Sending;
using Contour.Serialization;

namespace Contour.Transport.Kafka.Configuration;

/// <summary>
///   Настройки отправителя сообщений в топик Kafka.
/// </summary>
public record KafkaSenderOptions : ISenderConfiguration
{
    /// <summary>
    ///   Свойства Confluent-продюсера, заданные напрямую по имени librdkafka.
    ///   Строго типизированные настройки применяются отдельно, через <see cref="ProducerConfig"/>,
    ///   чтобы их сериализацией занимался сам Confluent.
    /// </summary>
    private Dictionary<string, string> rawProducerProperties = new();

    public string Key { get; internal set; }

    /// <summary>
    ///   Метка отправляемых сообщений; её имя используется как имя топика.
    /// </summary>
    public MessageLabel Label { get; internal set; }

    private string bootstrapServers;

    public string ConnectionString
    {
        get => BootstrapServersProvider?.GetConnectionString(Label) ?? this.bootstrapServers;
        internal set => this.bootstrapServers = value;
    }

    /// <summary>
    ///   Провайдер адресов брокеров: если задан, его значение важнее заданного явно.
    /// </summary>
    public IConnectionStringProvider BootstrapServersProvider { get; internal set; }

    public IPayloadConverter PayloadConverter { get; internal set; }

    /// <summary>
    ///   Требуется ли дождаться отчёта о доставке от брокера. При <c>false</c> отправка
    ///   становится «выстрелил и забыл»: сообщение уходит в буфер продюсера.
    /// </summary>
    public bool ConfirmationRequired { get; internal set; } = true;

    /// <summary>
    ///   Время ожидания отчёта о доставке, <c>null</c> — ждать столько, сколько позволяет
    ///   <c>message.timeout.ms</c> самого продюсера.
    /// </summary>
    public TimeSpan? ConfirmationTimeout { get; internal set; }

    /// <summary>
    ///   Сколько раз повторить отправку при ошибке доставки.
    /// </summary>
    public int FailoverAttempts { get; internal set; } = 1;

    /// <summary>
    ///   Время ожидания при завершении работы, за которое продюсер дописывает буфер в брокер.
    /// </summary>
    public TimeSpan FlushTimeout { get; internal set; } = TimeSpan.FromSeconds(10);

    public Acks? Acks { get; internal set; }

    public CompressionType? CompressionType { get; internal set; }

    public bool? EnableIdempotence { get; internal set; }

    public TimeSpan? Linger { get; internal set; }

    public TimeSpan? MessageTimeout { get; internal set; }

    public SecurityProtocol? SecurityProtocol { get; internal set; }

    public string SslCaPem { get; internal set; }

    public string SslCertificatePem { get; internal set; }

    public string SslKeyPem { get; internal set; }

    public bool? EnableSslCertificateVerification { get; internal set; }

    public string Debug { get; internal set; }

    /// <summary>
    ///   Заголовки <c>x-persist</c>, <c>x-delay</c> и <c>x-ttl</c> Kafka не интерпретирует,
    ///   поэтому настраивать их у маршрута нечем: свойства существуют только потому,
    ///   что их требует <see cref="ISenderConfiguration"/>, и всегда остаются пустыми.
    /// </summary>
    public bool Persistently => false;

    /// <inheritdoc cref="Persistently"/>
    public bool Delayed => false;

    /// <inheritdoc cref="Persistently"/>
    public TimeSpan? Ttl => null;

    public TimeSpan? RequestTimeout { get; internal set; }

    /// <summary>
    ///   Нужен ли этому маршруту топик ответов: без него <c>RequestAsync</c> недоступен.
    /// </summary>
    public bool RequiresCallback { get; internal set; }

    /// <summary>
    ///   Топик, в который отвечают на запросы этого маршрута. Незаданный разрешается при
    ///   создании шины в <c>{endpoint}.{label}..reply</c>.
    /// </summary>
    public string CallbackTopic { get; internal set; }

    /// <summary>
    ///   Настройки получателя ответов маршрута запрос-ответа: читает <see cref="CallbackTopic"/>.
    /// </summary>
    public KafkaReceiverOptions CallbackOptions { get; internal set; }

    public IIncomingMessageHeaderStorage IncomingMessageHeaderStorage { get; internal set; }

    internal IProducerSelectorBuilder ProducerSelectorBuilder { get; set; }

    /// <summary>
    ///   Задаёт произвольное свойство Confluent-продюсера по имени librdkafka.
    /// </summary>
    internal void SetRawProducerProperty(string key, string value)
    {
        // Copy-on-write: маршруты порождаются из дефолтов через `with`, который копирует
        // только ссылку на словарь. Без копии настройка одного маршрута протекала бы в остальные.
        var updated = new Dictionary<string, string>(this.rawProducerProperties);

        if (value == null)
        {
            updated.Remove(key);
        }
        else
        {
            updated[key] = value;
        }

        this.rawProducerProperties = updated;
    }

    /// <summary>
    ///   Собирает конфигурацию Confluent-продюсера. Сначала применяются явно заданные
    ///   свойства librdkafka, затем строго типизированные настройки — они имеют приоритет.
    /// </summary>
    internal ProducerConfig BuildProducerConfig()
    {
        var config = new ProducerConfig(new Dictionary<string, string>(this.rawProducerProperties))
        {
            BootstrapServers = ConnectionString
        };

        if (Acks.HasValue)
        {
            config.Acks = Acks.Value;
        }

        if (CompressionType.HasValue)
        {
            config.CompressionType = CompressionType.Value;
        }

        if (EnableIdempotence.HasValue)
        {
            config.EnableIdempotence = EnableIdempotence.Value;
        }

        if (Linger.HasValue)
        {
            config.LingerMs = Linger.Value.TotalMilliseconds;
        }

        if (MessageTimeout.HasValue)
        {
            config.MessageTimeoutMs = (int)MessageTimeout.Value.TotalMilliseconds;
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
}
