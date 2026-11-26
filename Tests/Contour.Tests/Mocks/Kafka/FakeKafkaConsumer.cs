using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Contour.Transport.Kafka.Internal;
using KafkaHeaders = Confluent.Kafka.Headers;

namespace Contour.Tests.Mocks.Kafka;

/// <summary>
///   Фейковый Confluent-консьюмер: сообщения кладутся в очередь через <see cref="Enqueue"/>,
///   Consume блокируется на очереди и честно реагирует на отмену токена.
/// </summary>
internal sealed class FakeKafkaConsumer : IConsumer<Ignore, byte[]>
{
    private readonly BlockingCollection<Func<ConsumeResult<Ignore, byte[]>>> queue = new();

    private readonly List<TopicPartitionOffset> commits = new();

    private readonly List<TopicPartitionOffset> storedOffsets = new();

    private readonly List<string> subscribedTopics = new();

    /// <summary>Назначенные партиции — те, в которые клали сообщения.</summary>
    private readonly HashSet<TopicPartition> assignment = new();

    private readonly HashSet<TopicPartition> paused = new();

    private int pauseCount;

    private int resumeCount;

    /// <summary>Сколько раз чтение приостанавливали.</summary>
    public int PauseCount => Volatile.Read(ref this.pauseCount);

    /// <summary>Сколько раз чтение возобновляли.</summary>
    public int ResumeCount => Volatile.Read(ref this.resumeCount);

    /// <summary>Приостановлена ли хотя бы одна партиция: тогда Consume ничего не отдаёт.</summary>
    public bool IsPaused
    {
        get
        {
            lock (this.assignment)
            {
                return this.paused.Count > 0;
            }
        }
    }

    public bool IsClosed { get; private set; }

    public bool IsDisposed { get; private set; }

    /// <summary>Потокобезопасный снимок: Subscribe зовётся с потока воркера, ассерты — с тестового.</summary>
    public IReadOnlyList<string> SubscribedTopics
    {
        get
        {
            lock (this.subscribedTopics)
            {
                return this.subscribedTopics.ToArray();
            }
        }
    }

    /// <summary>
    ///   Колбэк отзыва партиций, который зарегистрировал листенер (проставляется фабрикой).
    /// </summary>
    public PartitionsRevokedHandler PartitionsRevokedHandler { get; set; }

    /// <summary>
    ///   Позиции, сохранённые для автокоммита librdkafka (StoreOffset).
    /// </summary>
    public IReadOnlyList<TopicPartitionOffset> StoredOffsets
    {
        get
        {
            lock (this.storedOffsets)
            {
                return this.storedOffsets.ToArray();
            }
        }
    }

    public IReadOnlyList<TopicPartitionOffset> Commits
    {
        get
        {
            lock (this.commits)
            {
                return this.commits.ToArray();
            }
        }
    }

    public void Enqueue(string topic, long offset, string payload, int partition = 0)
    {
        EnqueueRaw(topic, offset, Encoding.UTF8.GetBytes(payload), null, partition);
    }

    /// <summary>
    ///   Кладёт сообщение как есть — с байтами и заголовками Kafka.
    /// </summary>
    public void EnqueueRaw(string topic, long offset, byte[] value, KafkaHeaders headers, int partition = 0)
    {
        var result = new ConsumeResult<Ignore, byte[]>
        {
            Message = new Message<Ignore, byte[]> { Value = value, Headers = headers ?? new KafkaHeaders() },
            TopicPartitionOffset = new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset))
        };

        lock (this.assignment)
        {
            this.assignment.Add(result.TopicPartition);
        }

        this.queue.Add(() => result);
    }

    /// <summary>
    ///   Имитация ребаланса: отзыв партиций происходит на потоке воркера внутри Consume —
    ///   так же, как это делает librdkafka.
    /// </summary>
    public void EnqueueRevoke(params TopicPartitionOffset[] revokedPartitions)
    {
        this.queue.Add(() =>
        {
            PartitionsRevokedHandler?.Invoke(this, new List<TopicPartitionOffset>(revokedPartitions));
            return null;
        });
    }

    /// <summary>
    ///   Следующий вызов Consume бросит указанное исключение (имитация фатальной ошибки транспорта).
    /// </summary>
    public void FailNextConsume(Exception exception)
    {
        this.queue.Add(() => throw exception);
    }

    /// <summary>
    ///   Каждый вызов Consume бросает это исключение — имитация устойчивой ошибки чтения
    ///   (брокер недоступен, отказ авторизации), которая возвращается мгновенно.
    /// </summary>
    public Exception PermanentConsumeException { get; set; }

    /// <summary>Сколько раз был вызван Consume: для проверки паузы между попытками.</summary>
    public int ConsumeCount => Volatile.Read(ref this.consumeCount);

    private int consumeCount;

    /// <summary>
    ///   Следующий коммит сорвётся: позиция должна быть предложена на коммит снова.
    /// </summary>
    public void FailNextCommit(Exception exception)
    {
        this.nextCommitException = exception;
    }

    private Exception nextCommitException;

    public ConsumeResult<Ignore, byte[]> Consume(CancellationToken cancellationToken = default)
    {
        ThrowIfPermanentlyFailing();

        return this.queue.Take(cancellationToken)();
    }

    public ConsumeResult<Ignore, byte[]> Consume(int millisecondsTimeout)
    {
        ThrowIfPermanentlyFailing();

        return this.queue.TryTake(out var thunk, millisecondsTimeout) ? thunk() : null;
    }

    public ConsumeResult<Ignore, byte[]> Consume(TimeSpan timeout)
    {
        ThrowIfPermanentlyFailing();

        // Как librdkafka: с приостановленных партиций сообщения не приходят, но poll идёт.
        if (IsPaused)
        {
            Thread.Sleep(timeout);
            return null;
        }

        return this.queue.TryTake(out var thunk, timeout) ? thunk() : null;
    }

    private void ThrowIfPermanentlyFailing()
    {
        Interlocked.Increment(ref this.consumeCount);

        if (PermanentConsumeException != null)
        {
            throw PermanentConsumeException;
        }
    }

    public void Subscribe(string topic)
    {
        lock (this.subscribedTopics)
        {
            this.subscribedTopics.Add(topic);
        }
    }

    public void Subscribe(IEnumerable<string> topics)
    {
        lock (this.subscribedTopics)
        {
            this.subscribedTopics.AddRange(topics);
        }
    }

    public void Commit(ConsumeResult<Ignore, byte[]> result)
    {
        lock (this.commits)
        {
            this.commits.Add(result.TopicPartitionOffset);
        }
    }

    public void Commit(IEnumerable<TopicPartitionOffset> offsets)
    {
        var failure = Interlocked.Exchange(ref this.nextCommitException, null);

        if (failure != null)
        {
            throw failure;
        }

        lock (this.commits)
        {
            this.commits.AddRange(offsets);
        }
    }

    public List<TopicPartitionOffset> Commit()
    {
        return new List<TopicPartitionOffset>();
    }

    public void Close()
    {
        IsClosed = true;
    }

    public void Dispose()
    {
        IsDisposed = true;
    }

    public void Unsubscribe()
    {
    }

    public void Assign(TopicPartition partition)
    {
    }

    public void Assign(TopicPartitionOffset partition)
    {
    }

    public void Assign(IEnumerable<TopicPartitionOffset> partitions)
    {
    }

    public void Assign(IEnumerable<TopicPartition> partitions)
    {
    }

    public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions)
    {
    }

    public void IncrementalAssign(IEnumerable<TopicPartition> partitions)
    {
    }

    public void IncrementalUnassign(IEnumerable<TopicPartition> partitions)
    {
    }

    public void Unassign()
    {
    }

    public void StoreOffset(ConsumeResult<Ignore, byte[]> result)
    {
    }

    public void StoreOffset(TopicPartitionOffset offset)
    {
        lock (this.storedOffsets)
        {
            this.storedOffsets.Add(offset);
        }
    }

    public void Seek(TopicPartitionOffset tpo)
    {
    }

    public void Pause(IEnumerable<TopicPartition> partitions)
    {
        lock (this.assignment)
        {
            this.paused.UnionWith(partitions);
        }

        Interlocked.Increment(ref this.pauseCount);
    }

    public void Resume(IEnumerable<TopicPartition> partitions)
    {
        lock (this.assignment)
        {
            this.paused.ExceptWith(partitions);
        }

        Interlocked.Increment(ref this.resumeCount);
    }

    public List<TopicPartitionOffset> Committed(TimeSpan timeout)
    {
        return new List<TopicPartitionOffset>();
    }

    public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
    {
        return new List<TopicPartitionOffset>();
    }

    public Offset Position(TopicPartition partition)
    {
        return Offset.Unset;
    }

    public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
    {
        return new List<TopicPartitionOffset>();
    }

    public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
    {
        return null;
    }

    public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
    {
        return null;
    }

    public int AddBrokers(string brokers)
    {
        return 0;
    }

    public void SetSaslCredentials(string username, string password)
    {
    }

    public Handle Handle => null;

    public string Name => "fake-consumer";

    public string MemberId => "fake-member";

    public List<TopicPartition> Assignment
    {
        get
        {
            lock (this.assignment)
            {
                return this.assignment.ToList();
            }
        }
    }

    public List<string> Subscription => new(SubscribedTopics);

    public IConsumerGroupMetadata ConsumerGroupMetadata => null;
}
