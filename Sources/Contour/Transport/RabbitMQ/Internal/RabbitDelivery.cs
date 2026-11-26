using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Contour.Configuration;
using Contour.Receiving;
using Contour.Sending;
using RabbitMQ.Client.Events;

namespace Contour.Transport.RabbitMQ.Internal;

/// <summary>
/// Доставленное сообщение через брокер <c>RabbitMQ</c>.
/// </summary>
internal class RabbitDelivery : IDelivery
{
    private readonly Lazy<IDictionary<string, object>> headers;

    private readonly bool requiresAccept;
    private readonly RabbitBus bus;
    private volatile bool isAccepted;

    public RabbitDelivery(RabbitBus bus, RabbitChannel channel, BasicDeliverEventArgs args, bool requiresAccept)
    {
        this.bus = bus;

        Channel = channel;
        Label = this.bus.MessageLabelHandler.Resolve(args);
        Args = args;
        this.requiresAccept = requiresAccept;

        this.headers = new Lazy<IDictionary<string, object>>(
            () => ExtractHeadersFrom(args));
    }

    public BasicDeliverEventArgs Args { get; private set; }

    public RabbitChannel Channel { get; private set; }

    public string CorrelationId => Args.BasicProperties.CorrelationId;

    public IDictionary<string, object> Headers => this.headers.Value;

    public bool IsResponse => CorrelationId != null && ReplyRoute == null;

    public MessageLabel Label { get; private set; }

    public bool CanReply => ReplyRoute != null && !string.IsNullOrWhiteSpace(CorrelationId);

    /// <summary>
    /// Маршрут ответа на доставленное сообщение.
    /// </summary>
    private RabbitRoute ReplyRoute
    {
        get
        {
            if (Args.BasicProperties.ReplyTo == null)
            {
                return null;
            }

            var replyAddress = Args.BasicProperties.ReplyToAddress;
            return new RabbitRoute(replyAddress.ExchangeName, replyAddress.RoutingKey);
        }
    }

    public async Task AcceptAsync(CancellationToken cancellationToken = default)
    {
        if (!this.requiresAccept || this.isAccepted)
        {
            return;
        }

        await Channel.Accept(this, cancellationToken);
        this.isAccepted = true;
    }

    public IConsumingContext<T> BuildConsumingContext<T>(MessageLabel label = null) where T : class
    {
        Message<T> message = UnpackAs<T>();

        if (label != null && !label.IsAny && !label.IsEmpty)
        {
            message = message with
            {
                Label = label
            };
        }

        return new DefaultConsumingContext<T>(this.bus, message, this);
    }

    public Task ForwardAsync(MessageLabel label, object payload)
    {
        var headers = new Dictionary<string, object>(Headers);
        headers[Contour.Headers.CorrelationId] = CorrelationId;
        headers[Contour.Headers.ReplyRoute] = ReplyRoute;
        if (!string.IsNullOrEmpty(Args.BasicProperties.Expiration) && long.TryParse(Args.BasicProperties.Expiration, out var expiration))
        {
            headers[Contour.Headers.Ttl] = TimeSpan.FromMilliseconds(expiration);
        }
        Contour.Headers.ApplyBreadcrumbs(headers, this.bus.Endpoint.Address);
        Contour.Headers.ApplyOriginalMessageId(headers);
        headers[Contour.Headers.Forwarded] = true;

        // TODO это костыль, как и само использование константы "document.contour.failed", возможно правильно было бы иметь отдельный метод ForwardToFault
        return this.bus.EmitAsync(label, payload, new PublishingOptions
        {
            AdditionalHeaders = headers,
            ForceConnectionKey = string.Equals(label.Name, SystemQueues.Fault, StringComparison.OrdinalIgnoreCase) || string.Equals(label.Name, SystemQueues.Unhandled, StringComparison.OrdinalIgnoreCase)
                ? null
                : Channel.ConnectionKey
        });
    }

    public async Task RejectAsync(bool requeue, CancellationToken cancellationToken = default)
    {
        if (!this.requiresAccept || this.isAccepted)
        {
            return;
        }

        await Channel.Reject(this, requeue, cancellationToken);
        this.isAccepted = true;
    }

    public async Task ReplyWithAsync<TReply>(Message<TReply> message, CancellationToken cancellationToken = default) where TReply : class
    {
        if (!CanReply)
        {
            throw new BusConfigurationException("No ReplyToAddress or CorrelationId were found in delivery. Make sure that you have callback endpoint set.");
        }

        await Channel.Reply(message, ReplyRoute, CorrelationId, cancellationToken);
    }

    public Message<T> UnpackAs<T>() where T : class
    {
        return Channel.UnpackAs<T>(this);
    }

    private IDictionary<string, object> ExtractHeadersFrom(BasicDeliverEventArgs args)
    {
        var h = args.BasicProperties.Headers != null
            ? new Dictionary<string, object>(args.BasicProperties.Headers)
            : new Dictionary<string, object>();

        if (CorrelationId != null)
        {
            h[Contour.Headers.CorrelationId] = CorrelationId;
        }

        if (ReplyRoute != null)
        {
            h[Contour.Headers.ReplyRoute] = ReplyRoute;
        }

        return h;
    }
}
