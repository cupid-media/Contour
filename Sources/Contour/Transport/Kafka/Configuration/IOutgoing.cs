using System;
using Confluent.Kafka;

namespace Contour.Transport.Kafka.Configuration;

public interface IOutgoing
{
    string Key { get; }

    string Label { get; }

    string ConnectionString { get; }

    bool? Confirm { get; }

    TimeSpan? ConfirmTimeout { get; }

    int? FailoverAttempts { get; }

    /// <summary>
    ///   Время, отведённое продюсеру на дозапись буфера при остановке шины.
    /// </summary>
    TimeSpan? FlushTimeout { get; }

    bool RequiresCallback { get; }

    string CallbackLabel { get; }

    /// <summary>
    ///   Время ожидания ответа на запрос.
    /// </summary>
    TimeSpan? RequestTimeout { get; }

    Acks? Acks { get; }

    CompressionType? CompressionType { get; }

    bool? EnableIdempotence { get; }

    /// <summary>
    ///  Время, которое продюсер копит сообщения в батч перед отправкой.
    /// </summary>
    TimeSpan? Linger { get; }

    /// <summary>
    ///  Полное время, отведённое librdkafka на доставку одного сообщения, включая повторы.
    /// </summary>
    TimeSpan? MessageTimeout { get; }

    SecurityProtocol? SecurityProtocol { get; }

    string SslCaPem { get; }

    string SslCertificatePem { get; }

    string SslKeyPem { get; }

    bool? EnableSslCertificateVerification { get; }

    string Debug { get; }
}
