using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Transport.Kafka.Internal;

namespace Contour.Tests.Mocks.Kafka;

/// <summary>
///   Фейковый Confluent-продюсер: запоминает отправленное и позволяет управлять
///   отчётом о доставке — сразу, с задержкой или с ошибкой.
/// </summary>
internal class FakeKafkaProducer : IProducer<string, byte[]>
{
    private readonly ConcurrentQueue<ProducedMessage> produced = new();

    private readonly List<Action> pendingReports = [];

    public IReadOnlyList<ProducedMessage> Produced => this.produced.ToArray();

    public bool IsDisposed { get; private set; }

    public int FlushCount { get; private set; }

    public Action<Error> ErrorHandler { get; set; }

    /// <summary>Задерживать отчёты о доставке до вызова <see cref="ReleaseReports"/>.</summary>
    public bool HoldReports { get; set; }

    /// <summary>Отчёт о доставке будет содержать эту ошибку.</summary>
    public Error DeliveryError { get; set; }

    /// <summary>Следующий Produce бросит это исключение.</summary>
    public Exception ProduceException { get; set; }

    public void ReleaseReports()
    {
        Action[] reports;

        lock (this.pendingReports)
        {
            reports = this.pendingReports.ToArray();
            this.pendingReports.Clear();
        }

        foreach (var report in reports)
        {
            report();
        }
    }

    public virtual void Produce(string topic, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>> deliveryHandler = null)
    {
        if (ProduceException != null)
        {
            var toThrow = ProduceException;
            ProduceException = null;
            throw toThrow;
        }

        this.produced.Enqueue(new ProducedMessage(topic, message.Key, message.Value, KafkaHeaderConverter.FromKafka(message.Headers)));

        if (deliveryHandler == null)
        {
            return;
        }

        var report = new DeliveryReport<string, byte[]>
        {
            Message = message,
            TopicPartitionOffsetError = new TopicPartitionOffsetError(
                topic,
                new Partition(0),
                new Offset(this.produced.Count - 1),
                DeliveryError ?? new Error(ErrorCode.NoError))
        };

        void Deliver() => deliveryHandler(report);

        if (HoldReports)
        {
            lock (this.pendingReports)
            {
                this.pendingReports.Add(Deliver);
            }

            return;
        }

        Deliver();
    }

    public void Produce(TopicPartition topicPartition, Message<string, byte[]> message, Action<DeliveryReport<string, byte[]>> deliveryHandler = null)
    {
        Produce(topicPartition.Topic, message, deliveryHandler);
    }

    public int Flush(TimeSpan timeout)
    {
        FlushCount++;
        ReleaseReports();
        return 0;
    }

    public void Flush(CancellationToken cancellationToken = default)
    {
        FlushCount++;
        ReleaseReports();
    }

    public void Dispose()
    {
        IsDisposed = true;
    }

    public Task<DeliveryResult<string, byte[]>> ProduceAsync(
        string topic,
        Message<string, byte[]> message,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("KafkaProducer must use Produce with a delivery report handler");
    }

    public Task<DeliveryResult<string, byte[]>> ProduceAsync(
        TopicPartition topicPartition,
        Message<string, byte[]> message,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("KafkaProducer must use Produce with a delivery report handler");
    }

    public int Poll(TimeSpan timeout) => 0;

    public void InitTransactions(TimeSpan timeout)
    {
    }

    public void BeginTransaction()
    {
    }

    public void CommitTransaction(TimeSpan timeout)
    {
    }

    public void CommitTransaction()
    {
    }

    public void AbortTransaction(TimeSpan timeout)
    {
    }

    public void AbortTransaction()
    {
    }

    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
    {
    }

    public void SetSaslCredentials(string username, string password)
    {
    }

    public int AddBrokers(string brokers) => 0;

    public Handle Handle => null;

    public string Name => "fake-producer";
}
