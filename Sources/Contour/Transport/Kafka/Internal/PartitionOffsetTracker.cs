using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Учёт позиций коммита по каждой партиции в пределах одного воркера.
///   Не потокобезопасен: и consume-цикл, и колбэк отзыва партиций выполняются на потоке воркера
///   (librdkafka вызывает rebalance-колбэки внутри Consume).
/// </summary>
/// <remarks>
///   Коммитится не «последний обработанный + 1», а наименьший полученный, но не подтверждённый
///   оффсет: иначе успешно обработанное сообщение сдвинуло бы позицию за неподтверждённое,
///   и то было бы потеряно. Пропуски в нумерации оффсетов (компактированные и транзакционные
///   топики) при этом безопасны, потому что учитываются только реально полученные оффсеты.
/// </remarks>
internal sealed class PartitionOffsetTracker
{
    private readonly Dictionary<TopicPartition, PartitionState> states = new();

    /// <summary>
    ///   Отмечает сообщение полученным: до подтверждения оно удерживает позицию коммита.
    /// </summary>
    public void Observe(ConsumeResult<Ignore, byte[]> consumeResult)
    {
        var state = StateOf(consumeResult.TopicPartition);
        var offset = consumeResult.Offset.Value;

        state.FirstObserved ??= offset;
        state.LastObserved = offset;
        state.Pending.Add(offset);
    }

    /// <summary>
    ///   Подтверждает обработку сообщения: позиция коммита может пройти дальше него.
    /// </summary>
    public void Acknowledge(ConsumeResult<Ignore, byte[]> consumeResult)
    {
        StateOf(consumeResult.TopicPartition).Pending.Remove(consumeResult.Offset.Value);
    }

    /// <summary>
    ///   Позиции для коммита по всем отслеживаемым партициям. Уже зафиксированная или
    ///   не сдвинувшаяся с начала чтения позиция не возвращается — коммитить её незачем.
    /// </summary>
    public IReadOnlyCollection<TopicPartitionOffset> GetCommitPositions()
    {
        var positions = new List<TopicPartitionOffset>();

        foreach (var pair in this.states)
        {
            if (TryGetPosition(pair.Value, out var position))
            {
                positions.Add(new TopicPartitionOffset(pair.Key, new Offset(position)));
            }
        }

        return positions;
    }

    /// <summary>
    ///   Отмечает позиции зафиксированными. Вызывается только после успешного коммита:
    ///   сорвавшийся коммит должен быть предложен снова, а не потерян.
    /// </summary>
    public void Committed(IEnumerable<TopicPartitionOffset> positions)
    {
        foreach (var position in positions)
        {
            if (this.states.TryGetValue(position.TopicPartition, out var state))
            {
                state.LastEmitted = position.Offset.Value;
            }
        }
    }

    /// <summary>
    ///   Изымает и возвращает позиции для коммита по указанным партициям — вызывается при их отзыве,
    ///   чтобы обработанный хвост не уехал повторно новому владельцу партиции.
    /// </summary>
    public IReadOnlyCollection<TopicPartitionOffset> DrainCommitPositions(IEnumerable<TopicPartition> partitions)
    {
        var positions = new List<TopicPartitionOffset>();

        foreach (var partition in partitions)
        {
            if (!this.states.Remove(partition, out var state))
            {
                continue;
            }

            if (TryGetPosition(state, out var position))
            {
                positions.Add(new TopicPartitionOffset(partition, new Offset(position)));
            }
        }

        return positions;
    }

    private static bool TryGetPosition(PartitionState state, out long position)
    {
        position = 0;

        if (state.LastObserved == null)
        {
            return false;
        }

        // Наименьший неподтверждённый удерживает позицию; если подтверждено всё —
        // читать дальше последнего полученного.
        position = state.Pending.Count > 0 ? state.Pending.Min : state.LastObserved.Value + 1;

        return position != state.LastEmitted && position > state.FirstObserved;
    }

    private PartitionState StateOf(TopicPartition partition)
    {
        if (!this.states.TryGetValue(partition, out var state))
        {
            state = new PartitionState();
            this.states[partition] = state;
        }

        return state;
    }

    private sealed class PartitionState
    {
        /// <summary>Полученные, но ещё не подтверждённые оффсеты.</summary>
        public SortedSet<long> Pending { get; } = [];

        public long? FirstObserved { get; set; }

        public long? LastObserved { get; set; }

        public long? LastEmitted { get; set; }
    }
}
