using System.Collections.Generic;
using Confluent.Kafka;
using Contour.Transport.Kafka.Configuration;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Вызывается при отзыве партиций (ребаланс) на потоке воркера внутри Consume,
///   до фактической передачи партиций другому участнику группы.
/// </summary>
internal delegate void PartitionsRevokedHandler(IConsumer<Ignore, byte[]> consumer, List<TopicPartitionOffset> revokedPartitions);

internal interface IKafkaConsumerFactory
{
    IConsumer<Ignore, byte[]> Create(KafkaReceiverOptions options, PartitionsRevokedHandler partitionsRevokedHandler);
}
