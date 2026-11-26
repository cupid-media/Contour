using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Contour.Configuration;

namespace Contour.Transport.Kafka.Configuration;

public interface IKafkaEndpoint
{
    string Name { get; }

    string ConnectionString { get; }

    /// <summary>
    /// Идентификатор группы, в которую входит эта шина. По умолчанию выводится из имени конечной точки.
    /// </summary>
    string GroupId { get; }

    uint? ParallelismLevel { get; }

    string LifecycleHandler { get; }

    string[] ExcludedHeaders { get; }

    TimeSpan? RequestTimeout { get; }

    int? FailoverAttempts { get; }

    bool? Confirm { get; }

    TimeSpan? ConfirmTimeout { get; }

    int? CommitOffset { get; }

    TimeSpan? CommitTimeout { get; }

    int? MaxPendingAcknowledgements { get; }

    AutoOffsetReset? AutoOffsetReset { get; }

    bool? EnableAutoCommit { get; }

    PartitionAssignmentStrategy? PartitionAssignmentStrategy { get; }

    SecurityProtocol? SecurityProtocol { get; }

    string SslCaPem { get; }

    string SslCertificatePem { get; }

    string SslKeyPem { get; }

    bool? EnableSslCertificateVerification { get; }

    string Debug { get; }

    IDynamic Dynamic { get; }

    IEnumerable<IIncoming> Incoming { get; }

    IEnumerable<IOutgoing> Outgoing { get; }
}
