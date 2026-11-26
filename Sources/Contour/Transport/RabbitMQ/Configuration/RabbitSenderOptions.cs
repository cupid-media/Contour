using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Transport.RabbitMQ.Configuration;

public record RabbitSenderOptions : ISenderConfiguration
{
    public string Key { get; internal set; }

    public MessageLabel Label { get; internal set; }

    public bool RequiresCallback { get; internal set; }

    public string ConnectionString
    {
        get => ConnectionStringProvider?.GetConnectionString(Label) ?? field;
        internal set;
    }

    public IConnectionStringProvider ConnectionStringProvider { get; internal set; }

    /// <summary>
    /// Требуется ли подтверждение об успешной отправке.
    /// </summary>
    public bool ConfirmationRequired { get; internal set; }

    /// <summary>
    /// Требуется ли сохранение сообщения.
    /// </summary>
    public bool Persistently { get; internal set; }

    public bool ReuseConnection { get; internal set; }

    public int FailoverAttempts { get; internal set; }

    /// <summary>
    /// Время ожидания ответа на запрос.
    /// </summary>
    public TimeSpan? RequestTimeout { get; internal set; }

    /// <summary>
    /// Построитель вычислителя маршрута.
    /// </summary>
    public Func<RabbitRouteResolverBuilderParams, CancellationToken, ValueTask<IRouteResolver>> RouteResolverBuilder { get; internal set; }

    public bool RecreateRouteOnFailure { get; internal set; }

    /// <summary>
    /// Время жизни сообщения.
    /// </summary>
    public TimeSpan? Ttl { get; internal set; }

    /// <summary>
    /// Требуется ли отправка сообщений с задержкой
    /// </summary>
    public bool Delayed { get; internal set; }

    /// <summary>
    /// Время ожидания подтверждения получения сообщения, null если требуется бесконечное ожидание.
    /// </summary>
    public TimeSpan? ConfirmationTimeout { get; internal set; }

    /// <summary>
    /// Gets or internal sets the maximum time in seconds to wait before making the next attempt to send a message
    /// </summary>
    public int? MaxRetryDelay { get; internal set; }

    /// <summary>
    /// Gets or internal sets the time in seconds to wait before the retry delays will be reinternal set due to inactivity
    /// </summary>
    public int? InactivityResetDelay { get; internal set; }

    /// <summary>
    /// Defines an algorithm which specifies the way in which producers will be taken from the list by the sender.
    /// </summary>
    internal IProducerSelectorBuilder ProducerSelectorBuilder { get; set; }

    public IIncomingMessageHeaderStorage IncomingMessageHeaderStorage { get; internal set; }

    public RabbitReceiverOptions CallbackOptions { get; internal set; }
}
