using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Contour.Configuration;

namespace Contour.Transport.Kafka.Configuration;

public class KafkaEndpoint : IKafkaEndpoint
{
    public string Name { get; set; }

    public string ConnectionString { get; set; }

    public string GroupId { get; set; }

    public uint? ParallelismLevel { get; set; }

    public string LifecycleHandler { get; set; }

    public string[] ExcludedHeaders { get; set; }

    public TimeSpan? RequestTimeout { get; set; }

    public int? FailoverAttempts { get; set; }

    public bool? Confirm { get; set; }

    public TimeSpan? ConfirmTimeout { get; set; }

    public int? CommitOffset { get; set; }

    public TimeSpan? CommitTimeout { get; set; }

    public int? MaxPendingAcknowledgements { get; set; }

    public AutoOffsetReset? AutoOffsetReset { get; set; }

    public bool? EnableAutoCommit { get; set; }

    public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get; set; }

    public SecurityProtocol? SecurityProtocol { get; set; }

    public string SslCaPem { get; set; }

    public string SslCertificatePem { get; set; }

    public string SslKeyPem { get; set; }

    public bool? EnableSslCertificateVerification { get; set; }

    public string Debug { get; set; }

    public IDynamic Dynamic { get; set; }

    public IEnumerable<IIncoming> Incoming { get; set; } = [];

    public IEnumerable<IOutgoing> Outgoing { get; set; } = [];
}
