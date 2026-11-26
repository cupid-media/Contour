using System;

namespace Contour.Transport.RabbitMQ.Topology;

/// <summary>
/// Построитель очереди.
/// </summary>
public class QueueBuilder
{
    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="QueueBuilder"/>.
    /// </summary>
    /// <param name="name">
    /// Имя очереди.
    /// </param>
    internal QueueBuilder(string name)
    {
        Instance = new Queue(name);
    }

    /// <summary>
    /// Очередь должна удаляться автоматически.
    /// </summary>
    public QueueBuilder WithAutoDelete()
    {
        Instance.AutoDelete = true;
        return this;
    }

    /// <summary>
    /// Очередь должна быть надежной.
    /// </summary>
    public QueueBuilder Durable()
    {
        Instance.Durable = true;
        return this;
    }

    /// <summary>
    /// Очередь должна быть эксклюзивной.
    /// </summary>
    public QueueBuilder Exclusive()
    {
        Instance.Exclusive = true;
        return this;
    }

    /// <summary>
    /// Экземпляр очереди.
    /// </summary>
    internal Queue Instance { get; private set; }

    /// <summary>
    /// Добавляет в построитель настройки времени жизни сообщений в очереди.
    /// </summary>
    /// <param name="ttl">Время жизни сообщений в очереди.</param>
    /// <returns>Построитель очереди.</returns>
    public QueueBuilder WithTtl(TimeSpan ttl)
    {
        Instance.Ttl = ttl;
        return this;
    }

    /// <summary>
    /// Добавляет в построитель настройки максимального количества сообщений в очереди.
    /// </summary>
    /// <param name="queueLimit">Максимальное количество сообщений в очереди.</param>
    /// <returns>Построитель очереди.</returns>
    public QueueBuilder WithLimit(int queueLimit)
    {
        Instance.Limit = queueLimit;
        return this;
    }

    /// <summary>
    /// Добавляет в построитель настройки максимального количества байт, которое занимает очередь
    /// </summary>
    /// <param name="bytes">Максимальное количество сообщений в очереди.</param>
    /// <returns>Построитель очереди.</returns>
    public QueueBuilder WithMaxLengthBytes(int bytes)
    {
        Instance.MaxLengthBytes = bytes;
        return this;
    }
}