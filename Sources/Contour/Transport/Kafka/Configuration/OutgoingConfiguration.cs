using System;
using Confluent.Kafka;

namespace Contour.Transport.Kafka.Configuration;

public class OutgoingConfiguration : IOutgoing
{
    public string Label { get; set; }

    public string Key { get; set; }

    public string ConnectionString { get; set; }

    public bool? Confirm { get; set; }

    public TimeSpan? ConfirmTimeout { get; set; }

    public int? FailoverAttempts { get; set; }

    public TimeSpan? FlushTimeout { get; set; }

    public bool RequiresCallback { get; set; }

    public string CallbackLabel { get; set; }

    public TimeSpan? RequestTimeout { get; set; }

    public Acks? Acks { get; set; }

    public CompressionType? CompressionType { get; set; }

    public bool? EnableIdempotence { get; set; }

    public TimeSpan? Linger { get; set; }

    public TimeSpan? MessageTimeout { get; set; }

    public SecurityProtocol? SecurityProtocol { get; set; }

    public string SslCaPem { get; set; }

    public string SslCertificatePem { get; set; }

    public string SslKeyPem { get; set; }

    public bool? EnableSslCertificateVerification { get; set; }

    public string Debug { get; set; }
}
