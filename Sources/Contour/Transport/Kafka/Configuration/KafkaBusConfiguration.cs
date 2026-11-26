using System.Collections.Generic;
using Contour.Configuration;
using Contour.Transport.Kafka.Internal;

namespace Contour.Transport.Kafka.Configuration;

internal record KafkaBusConfiguration : BusConfigurationBase
{
    public string ConnectionString { get; init; }

    public IReadOnlyCollection<KafkaReceiverOptions> ReceiverConfigurations { get; init; } = [];

    public IReadOnlyCollection<KafkaSenderOptions> SenderConfigurations { get; init; } = [];

    public KafkaReceiverOptions ReceiverDefaults { get; init; }

    public KafkaSenderOptions SenderDefaults { get; init; }

    public IKafkaConsumerFactory ConsumerFactory { get; init; }

    public IKafkaProducerFactory ProducerFactory { get; init; }
}
