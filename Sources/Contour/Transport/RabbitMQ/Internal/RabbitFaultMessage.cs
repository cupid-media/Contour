using System;
using System.Collections.Generic;
using System.Text;
using Contour.Helpers;

namespace Contour.Transport.RabbitMQ.Internal;

/// <summary>
/// The rabbit fault message.
/// </summary>
internal class RabbitFaultMessage : FaultMessage
{
    /// <summary>
    /// The text based content types.
    /// </summary>
    private static readonly ISet<string> TextBasedContentTypes = new HashSet<string> { "text/plain", "text/html", "text/xml", "application/json", "application/xml" };

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="RabbitFaultMessage"/>.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    /// <param name="exception">
    /// The exception.
    /// </param>
    public RabbitFaultMessage(RabbitDelivery delivery, Exception exception)
        : base(
            new Message<object>(delivery.Label,
                TextBasedContentTypes.Contains(delivery.Args.BasicProperties.ContentType)
                    ? Encoding.UTF8.GetString(delivery.Args.Body.ToArray())
                    : delivery.Args.Body.ToArray(), delivery.Headers),
            DateTimeExtensions.FromUnixTimestamp(delivery.Args.BasicProperties.Timestamp.UnixTime),
            delivery.Args.BasicProperties.ContentType,
            exception)
    {
        Route = new RabbitRoute(delivery.Args.Exchange, delivery.Args.RoutingKey);
    }

    public RabbitFaultMessage(RabbitDelivery delivery)
        : this(delivery, null)
    {
    }

    /// <summary>
    /// Gets the route.
    /// </summary>
    public RabbitRoute Route { get; private set; }
}