using System;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Serialization;

namespace Contour.Transport.Kafka.Configuration;

/// <summary>
///   Конфигуратор отправителя сообщений в топик. Метка маршрута используется как имя топика.
/// </summary>
public class KafkaSenderConfigurator
{
    internal KafkaSenderConfigurator(MessageLabel label, KafkaSenderOptions options)
    {
        Options = options with { Label = label };
        Label = label;
    }

    public MessageLabel Label { get; }

    /// <summary>
    ///   Настройки отправителя.
    /// </summary>
    internal KafkaSenderOptions Options { get; }

    /// <summary>Задаёт псевдоним метки, по которому можно отправлять сообщения.</summary>
    public KafkaSenderConfigurator WithKey(string key)
    {
        Options.Key = key?.StartsWith(":", StringComparison.Ordinal) == false ? ":" + key : key;
        return this;
    }

    public KafkaSenderConfigurator WithBootstrapServers(string bootstrapServers)
    {
        Options.ConnectionString = bootstrapServers;
        return this;
    }

    public KafkaSenderConfigurator WithPayloadConverter(IPayloadConverter converter)
    {
        Options.PayloadConverter = converter;
        return this;
    }

    /// <summary>
    ///   Дожидаться ли отчёта о доставке от брокера. При <c>false</c> отправка возвращает
    ///   управление сразу после постановки сообщения в буфер продюсера.
    /// </summary>
    public KafkaSenderConfigurator WithConfirmation(bool required = true, TimeSpan? timeout = null)
    {
        Options.ConfirmationRequired = required;

        if (timeout.HasValue)
        {
            if (timeout.Value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeout), "Confirmation timeout must be positive");
            }

            Options.ConfirmationTimeout = timeout;
        }

        return this;
    }

    public KafkaSenderConfigurator WithFailoverAttempts(int failoverAttempts)
    {
        if (failoverAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(failoverAttempts), "Failover attempts must be positive");
        }

        Options.FailoverAttempts = failoverAttempts;
        return this;
    }

    /// <summary>
    ///   Время, отведённое продюсеру на дозапись буфера в брокер при остановке шины.
    /// </summary>
    public KafkaSenderConfigurator WithFlushTimeout(TimeSpan flushTimeout)
    {
        if (flushTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(flushTimeout), "Flush timeout must be positive");
        }

        Options.FlushTimeout = flushTimeout;
        return this;
    }

    /// <summary>
    ///   Разрешает запрос-ответ на этом маршруте, используя топик ответов по умолчанию —
    ///   <c>{endpoint}.{label}..reply</c>. Выделенный топик задаёт
    ///   <see cref="WithCallbackTopic"/>.
    /// </summary>
    public KafkaSenderConfigurator WithDefaultCallbackEndpoint()
    {
        Options.RequiresCallback = true;
        Options.CallbackTopic = null;
        return this;
    }

    /// <summary>
    ///   Разрешает запрос-ответ на этом маршруте с выделенным топиком ответов.
    /// </summary>
    public KafkaSenderConfigurator WithCallbackTopic(string topic)
    {
        Options.RequiresCallback = true;
        Options.CallbackTopic = KafkaTopic.Validated(topic, nameof(topic));
        return this;
    }

    /// <summary>
    ///   Время ожидания ответа на запрос по умолчанию.
    /// </summary>
    public KafkaSenderConfigurator WithRequestTimeout(TimeSpan? requestTimeout)
    {
        if (requestTimeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(requestTimeout), "Request timeout must be positive");
        }

        Options.RequestTimeout = requestTimeout;
        return this;
    }

    public KafkaSenderConfigurator WithIncomingMessageHeaderStorage(IIncomingMessageHeaderStorage storage)
    {
        Options.IncomingMessageHeaderStorage = storage;
        return this;
    }

    /// <summary>
    ///   Подтверждения от брокера: <c>None</c> — без подтверждения, <c>Leader</c> — только лидер,
    ///   <c>All</c> — все синхронные реплики.
    /// </summary>
    public KafkaSenderConfigurator WithAcks(Acks acks)
    {
        Options.Acks = acks;
        return this;
    }

    public KafkaSenderConfigurator WithCompression(CompressionType compressionType)
    {
        Options.CompressionType = compressionType;
        return this;
    }

    /// <summary>
    ///   Включает идемпотентную отправку: брокер отбрасывает дубликаты при повторах.
    /// </summary>
    public KafkaSenderConfigurator WithIdempotence(bool enable = true)
    {
        Options.EnableIdempotence = enable;
        return this;
    }

    /// <summary>
    ///   Время, которое продюсер копит сообщения в батч перед отправкой.
    /// </summary>
    public KafkaSenderConfigurator WithLinger(TimeSpan linger)
    {
        if (linger < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(linger), "Linger must be non-negative");
        }

        Options.Linger = linger;
        return this;
    }

    /// <summary>
    ///   Полное время, отведённое librdkafka на доставку одного сообщения, включая повторы.
    /// </summary>
    public KafkaSenderConfigurator WithMessageTimeout(TimeSpan messageTimeout)
    {
        if (messageTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(messageTimeout), "Message timeout must be positive");
        }

        Options.MessageTimeout = messageTimeout;
        return this;
    }

    public KafkaSenderConfigurator WithSecurityProtocol(SecurityProtocol protocol)
    {
        Options.SecurityProtocol = protocol;
        return this;
    }

    public KafkaSenderConfigurator WithSslCaPem(string sslCaPem)
    {
        Options.SslCaPem = sslCaPem;
        return this;
    }

    public KafkaSenderConfigurator WithSslCertificatePem(string sslCertificatePem)
    {
        Options.SslCertificatePem = sslCertificatePem;
        return this;
    }

    public KafkaSenderConfigurator WithSslKeyPem(string sslKeyPem)
    {
        Options.SslKeyPem = sslKeyPem;
        return this;
    }

    public KafkaSenderConfigurator WithSslCertificateVerification(bool enable = true)
    {
        Options.EnableSslCertificateVerification = enable;
        return this;
    }

    public KafkaSenderConfigurator WithDebug(string debug)
    {
        Options.Debug = debug;
        return this;
    }

    /// <summary>
    ///   Задаёт произвольное свойство Confluent-продюсера по имени librdkafka —
    ///   для настроек, не покрытых остальными методами.
    /// </summary>
    public KafkaSenderConfigurator WithProducerProperty(string key, string value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Producer property name must be specified", nameof(key));
        }

        Options.SetRawProducerProperty(key, value);
        return this;
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Options.ConnectionString))
        {
            throw new BusConfigurationException(
                $"ConnectionString are not set for sender of [{Label}].");
        }

        if (Options.PayloadConverter == null)
        {
            throw new BusConfigurationException(
                $"PayloadConverter is not set for sender of [{Label}].");
        }

        if (Label.IsEmpty)
        {
            throw new BusConfigurationException("Sender label (topic) must be specified.");
        }
    }
}
