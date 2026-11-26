using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using KafkaHeaders = Confluent.Kafka.Headers;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.Kafka.Internal;

namespace Contour.Tests.Mocks.Kafka;

/// <summary>
///   Мини-брокер в памяти: связывает фейковых продюсеров и консьюмеров по имени топика,
///   чтобы прогонять полный цикл запрос-ответ без реальной Kafka.
///   Каждый топик хранит журнал, и подключившийся позже консьюмер получает его целиком —
///   поэтому тесты не зависят от того, кто первым успел подключиться.
/// </summary>
internal sealed class FakeKafkaBroker
{
    private readonly object syncRoot = new();

    private readonly Dictionary<string, List<StoredMessage>> logs = new();

    private readonly Dictionary<string, List<FakeKafkaConsumer>> consumers = new();

    public IKafkaConsumerFactory ConsumerFactory { get; }

    public IKafkaProducerFactory ProducerFactory { get; }

    public FakeKafkaBroker()
    {
        ConsumerFactory = new BrokerConsumerFactory(this);
        ProducerFactory = new BrokerProducerFactory(this);
    }

    /// <summary>Консьюмеры, подключённые к топику.</summary>
    public IReadOnlyList<FakeKafkaConsumer> Consumers(string topic)
    {
        lock (this.syncRoot)
        {
            return ConsumersOf(topic).ToArray();
        }
    }

    /// <summary>Публикует сообщение напрямую, минуя продюсера.</summary>
    public void Publish(string topic, byte[] value, KafkaHeaders headers = null)
    {
        Publish(topic, new Message<string, byte[]> { Value = value, Headers = headers ?? new KafkaHeaders() });
    }

    /// <summary>Все сообщения, опубликованные в топик.</summary>
    public IReadOnlyList<StoredMessage> Published(string topic)
    {
        lock (this.syncRoot)
        {
            return Log(topic).ToArray();
        }
    }

    internal sealed record StoredMessage(string Topic, long Offset, byte[] Value, KafkaHeaders Headers);

    private List<StoredMessage> Log(string topic)
    {
        if (!this.logs.TryGetValue(topic, out var log))
        {
            log = [];
            this.logs[topic] = log;
        }

        return log;
    }

    private List<FakeKafkaConsumer> ConsumersOf(string topic)
    {
        if (!this.consumers.TryGetValue(topic, out var list))
        {
            list = [];
            this.consumers[topic] = list;
        }

        return list;
    }

    private FakeKafkaConsumer RegisterConsumer(string topic)
    {
        var consumer = new FakeKafkaConsumer();

        lock (this.syncRoot)
        {
            // Догоняем журнал: сообщение могло быть опубликовано до подключения консьюмера.
            foreach (var message in Log(topic))
            {
                consumer.EnqueueRaw(message.Topic, message.Offset, message.Value, message.Headers);
            }

            ConsumersOf(topic).Add(consumer);
        }

        return consumer;
    }

    private void Publish(string topic, Message<string, byte[]> message)
    {
        lock (this.syncRoot)
        {
            var log = Log(topic);
            var stored = new StoredMessage(topic, log.Count, message.Value, message.Headers);
            log.Add(stored);

            foreach (var consumer in ConsumersOf(topic))
            {
                consumer.EnqueueRaw(stored.Topic, stored.Offset, stored.Value, stored.Headers);
            }
        }
    }

    private sealed class BrokerConsumerFactory(FakeKafkaBroker broker) : IKafkaConsumerFactory
    {
        public IConsumer<Ignore, byte[]> Create(
            KafkaReceiverOptions options,
            PartitionsRevokedHandler partitionsRevokedHandler)
        {
            var consumer = broker.RegisterConsumer(options.Label.Name);
            consumer.PartitionsRevokedHandler = partitionsRevokedHandler;
            return consumer;
        }
    }

    private sealed class BrokerProducerFactory(FakeKafkaBroker broker) : IKafkaProducerFactory
    {
        public Confluent.Kafka.IProducer<string, byte[]> Create(KafkaSenderOptions options, Action<Error> errorHandler)
        {
            return new BrokerProducer(broker);
        }
    }

    private sealed class BrokerProducer(FakeKafkaBroker broker) : FakeKafkaProducer
    {
        public override void Produce(
            string topic,
            Message<string, byte[]> message,
            Action<DeliveryReport<string, byte[]>> deliveryHandler = null)
        {
            broker.Publish(topic, message);
            base.Produce(topic, message, deliveryHandler);
        }
    }
}
