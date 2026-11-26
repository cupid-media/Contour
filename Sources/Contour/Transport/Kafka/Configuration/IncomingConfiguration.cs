using System;
using Confluent.Kafka;
using Contour.Configuration;

namespace Contour.Transport.Kafka.Configuration;

public class IncomingConfiguration : IIncoming
{
    public string Label { get; set; }

    public string ConnectionString { get; set; }

    public string GroupId { get; set; }

    public uint? ParallelismLevel { get; set; }

    public string Type { get; set; }

    public string React { get; set; }

    public Lifestyle? Lifestyle { get; set; }

    public bool? RequiresAccept { get; set; }

    public bool? Replies { get; set; }

    public AutoOffsetReset? AutoOffsetReset { get; set; }

    public bool? EnableAutoCommit { get; set; }

    public SecurityProtocol? SecurityProtocol { get; set; }

    public string SslCaPem { get; set; }

    public string SslCertificatePem { get; set; }

    public string SslKeyPem { get; set; }

    public bool? EnableSslCertificateVerification { get; set; }

    public string Debug { get; set; }

    public int? CommitOffset { get; set; }

    public TimeSpan? CommitTimeout { get; set; }

    public int? MaxPendingAcknowledgements { get; set; }

    public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get; set; }
}
