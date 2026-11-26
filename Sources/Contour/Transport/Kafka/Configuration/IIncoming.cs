using System;
using Confluent.Kafka;
using Contour.Configuration;

namespace Contour.Transport.Kafka.Configuration;

public interface IIncoming
{
    string ConnectionString { get; }

    string Label { get; }

    string GroupId { get; }

    uint? ParallelismLevel { get; }

    string Type { get; }

    string React { get; }

    Lifestyle? Lifestyle { get; }

    bool? RequiresAccept { get; }

    bool? Replies { get; }

    AutoOffsetReset? AutoOffsetReset { get; }

    bool? EnableAutoCommit { get; }

    SecurityProtocol? SecurityProtocol { get; }

    string SslCaPem { get; }

    string SslCertificatePem { get; }

    string SslKeyPem { get; }

    bool? EnableSslCertificateVerification { get; }

    string Debug { get; }

    int? CommitOffset { get; }

    TimeSpan? CommitTimeout { get; }

    int? MaxPendingAcknowledgements { get; }

    PartitionAssignmentStrategy? PartitionAssignmentStrategy { get; }
}
