using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;

namespace Contour.Transport.RabbitMQ.Configuration;

/// <summary>
/// Настройки получателя.
/// </summary>
public record RabbitReceiverOptions : IReceiverConfiguration
{
    public MessageLabel Label { get; internal set; }

    public Action<IReceiver> ReceiverRegistration { get; internal set; }

    public string ConnectionString
    {
        get => ConnectionStringProvider?.GetConnectionString(Label) ?? field;
        internal set;
    }

    public IConnectionStringProvider ConnectionStringProvider { get; internal set; }

    public QoSParams QoS { get; internal set; }

    public bool ReuseConnection { get; internal set; }

    /// <summary>
    /// Устанавливает необходимость явного подтверждения успешной обработки.
    /// </summary>
    public bool AcceptRequired { get; internal set; }

    /// <summary>
    /// Построитель порта получения входящих сообщений.
    /// </summary>
    public Func<RabbitSubscriptionEndpointBuilderParams, CancellationToken, Task<ISubscriptionEndpoint>> EndpointBuilder { get; internal set; }

    public bool RecreateEndpointOnFailure { get; internal set; }

    /// <summary>
    /// Обработчик сообщений, чья доставка завершилась провалом.
    /// </summary>
    public IFailedDeliveryStrategy FailedDeliveryStrategy { get; internal set; }

    /// <summary>
    /// Количество одновременных обработчиков сообщений.
    /// </summary>
    public uint? ParallelismLevel { get; internal set; }

    /// <summary>
    /// Длительность хранения сообщений в Fault очереди.
    /// </summary>
    public TimeSpan? FaultQueueTtl { get; internal set; }

    /// <summary>
    /// Максимальное количество сообщений в Fault очереди.
    /// </summary>
    public int? FaultQueueLimit { get; internal set; }

    /// <summary>
    /// Максимальное количество сообщений в потребляющей очереди.
    /// </summary>
    public int? QueueLimit { get; internal set; }

    /// <summary>
    /// Максимальное количество байт, которые занимают сообщения в потребляющей очереди.
    /// </summary>
    public int? QueueMaxLengthBytes { get; internal set; }

    /// <summary>
    /// Обработчик сообщений, для которых не найден потребитель.
    /// </summary>
    public IUnhandledDeliveryStrategy UnhandledDeliveryStrategy { get; internal set; }

    public bool Delayed { get; internal set; }

    public IReadOnlyCollection<string> ExcludedHeaders { get; internal set; }

    public IIncomingMessageHeaderStorage IncomingMessageHeaderStorage { get; internal set; }
}
