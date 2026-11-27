using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Contour.Helpers;
using Contour.Receiving;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Topology;
using RabbitMQ.Client;

namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitChannel : IAsyncDisposable
{
    private readonly IRabbitConnection connection;
    private readonly RabbitBus bus;
    private readonly SemaphoreSlim semaphore = new(1, 1);
    private readonly ILogger logger;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitChannel"/> class.
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="channel">A native transport channel</param>
    /// <param name="bus">A bus context</param>
    public RabbitChannel(IRabbitConnection connection, IChannel channel, RabbitBus bus)
    {
        this.connection = connection;
        Channel = channel;
        this.bus = bus;
        this.logger = LogManager.GetLogger($"{GetType().FullName}({this.connection.Id}, {GetHashCode()})");

        Channel.ChannelShutdownAsync += OnChannelShutdown;
    }

    /// <summary>
    /// Строка подключения
    /// </summary>
    internal string ConnectionString => this.connection.ConnectionString;

    public string ConnectionKey => this.connection.ConnectionKey;

    /// <summary>
    /// Is fired on channel shutdown.
    /// </summary>
    public AsyncEventWrapper<RabbitChannelShutdownEventArgs> Shutdown { get; } = new();

    /// <summary>
    /// Gets the native.
    /// </summary>
    protected IChannel Channel { get; }

    public bool IsOpen => Channel.IsOpen && this.connection.IsOpen;

    /// <summary>
    /// The accept.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Accept(RabbitDelivery delivery, CancellationToken cancellationToken)
    {
        this.logger.LogTrace("Accepting message [{Label}] ({DeliveryTag})", delivery.Label, delivery.Args.DeliveryTag);

        await SafeNativeInvoke(n => n.BasicAckAsync(delivery.Args.DeliveryTag, false, cancellationToken).AsTask(), cancellationToken);
    }

    /// <summary>
    /// The bind.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <param name="exchange">
    /// The exchange.
    /// </param>
    /// <param name="routingKey">
    /// The routing key.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Bind(Queue queue, Exchange exchange, string routingKey, CancellationToken cancellationToken)
    {
        try
        {
            await SafeNativeInvoke(n => n.QueueBindAsync(queue.Name, exchange.Name, routingKey, cancellationToken: cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            this.logger.LogError(e, "Failed to bind queue to exchange [{ExchangeName}:{QueueName}] on channel: {Channel}", exchange.Name, queue.Name, ToString());
            throw;
        }
    }

    public AsyncEventingBasicConsumer BuildConsumer()
    {
        return new AsyncEventingBasicConsumer(Channel);
    }

    /// <summary>
    /// The declare.
    /// </summary>
    /// <param name="exchange">
    /// The exchange.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Declare(Exchange exchange, CancellationToken cancellationToken)
    {
        try
        {
            await SafeNativeInvoke(
                n => n.ExchangeDeclareAsync(exchange.Name, exchange.Type, exchange.Durable, exchange.AutoDelete, exchange.Arguments, cancellationToken: cancellationToken),
                cancellationToken);
        }
        catch (Exception e)
        {
            this.logger.LogError(e, "Failed to Declare Exchange [{ExchangeName}] on channel: {Channel}", exchange.Name, ToString());
            throw;
        }
    }

    /// <summary>
    /// The declare.
    /// </summary>
    /// <param name="queue">
    /// The queue.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Declare(Queue queue, CancellationToken cancellationToken)
    {
        try
        {
            var arguments = new Dictionary<string, object>();
            if (queue.Ttl.HasValue)
            {
                arguments.Add(Headers.QueueMessageTtl, (long)queue.Ttl.Value.TotalMilliseconds);
            }

            if (queue.Limit.HasValue)
            {
                arguments.Add(Headers.QueueMaxLength, (int)queue.Limit);
            }

            if (queue.MaxLengthBytes.HasValue)
            {
                arguments.Add(Headers.QueueMaxLengthBytes, (int)queue.MaxLengthBytes);
            }

            await SafeNativeInvoke(n =>
                    n.QueueDeclareAsync(queue.Name, queue.Durable, queue.Exclusive, queue.AutoDelete, arguments, cancellationToken: cancellationToken),
                cancellationToken);
        }
        catch (Exception e)
        {
            this.logger.LogError(e, "Failed to Declare Queue [{QueueName}] on channel: {Channel}", queue.Name, ToString());
            throw;
        }
    }

    /// <summary>
    /// The declare default queue.
    /// </summary>
    /// <returns>
    /// The <see cref="string"/>.
    /// </returns>
    public async Task<string> DeclareDefaultQueue(CancellationToken cancellationToken)
    {
        try
        {
            var queueName = string.Empty;

            await SafeNativeInvoke(async n =>
            {
                var declareResult = await n.QueueDeclareAsync(cancellationToken: cancellationToken);
                queueName = declareResult.QueueName;
            }, cancellationToken);

            return queueName;
        }
        catch (Exception e)
        {
            this.logger.LogError(e, "Failed to Declare Default Queue on channel: {Channel}", ToString());
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (this.isDisposed)
        {
            return;
        }

        this.isDisposed = true;
        if (Channel != null)
        {
            try
            {
                if (IsOpen)
                {
                    await Channel.AbortAsync();
                }

                Channel.ChannelShutdownAsync -= OnChannelShutdown;
                Channel.Dispose();
            }
            catch (Exception)
            {
                // Suppress all errors here
            }
        }

        this.semaphore.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// The on confirmation.
    /// </summary>
    /// <param name="handleConfirmation">
    /// The handle confirmation.
    /// </param>
    public async Task OnConfirmation(ConfirmationHandler handleConfirmation, CancellationToken cancellationToken)
    {
        await SafeNativeInvoke(
            n =>
            {
                n.BasicAcksAsync += (model, args) => handleConfirmation(true, args.DeliveryTag, args.Multiple);
                n.BasicNacksAsync += (model, args) => handleConfirmation(false, args.DeliveryTag, args.Multiple);
                return Task.CompletedTask;
            },
            cancellationToken);
    }

    /// <summary>
    /// The publish.
    /// </summary>
    /// <param name="route">
    /// The route.
    /// </param>
    /// <param name="message">
    /// The message.
    /// </param>
    /// <param name="propsVisitor">
    /// The props visitor.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Publish<TRequest>(IRoute route,
        Message<TRequest> message,
        Func<IBasicProperties, IDictionary<string, object>> propsVisitor,
        CancellationToken cancellationToken) where TRequest : class
    {
        var (nativeRoute, body, props) = PrepareRequest(route, message, propsVisitor);

        await SafeNativeInvoke(
            n => n.BasicPublishAsync(nativeRoute.Exchange, nativeRoute.RoutingKey, false, props, body, cancellationToken).AsTask(),
            cancellationToken);
    }

    public async Task PublishWithConfirmation<TRequest>(IRoute route,
        Message<TRequest> message,
        Action<ulong> registerConfirmation,
        Func<IBasicProperties, IDictionary<string, object>> propsVisitor,
        CancellationToken cancellationToken) where TRequest : class
    {
        var (nativeRoute, body, props) = PrepareRequest(route, message, propsVisitor);

        await SafeNativeInvoke(async n =>
        {
            var nextSeq = await n.GetNextPublishSequenceNumberAsync(cancellationToken);
            registerConfirmation(nextSeq);
            await n.BasicPublishAsync(nativeRoute.Exchange, nativeRoute.RoutingKey, false, props, body, cancellationToken);
        }, cancellationToken);
    }

    private (RabbitRoute Route, byte[] Body, BasicProperties Props) PrepareRequest<TRequest>(IRoute route, Message<TRequest> message, Func<IBasicProperties, IDictionary<string, object>> propsVisitor) where TRequest : class
    {
        var nativeRoute = (RabbitRoute)route;

        this.logger.LogTrace("Emitting message [{Label}] ({Payload}) through [{Route}]", message.Label, message.Payload, nativeRoute);

        var props = new BasicProperties
        {
            ContentType = this.bus.PayloadConverter.ContentType,
            Timestamp = new AmqpTimestamp(DateTime.UtcNow.ToUnixTimestamp()),
            Headers = new Dictionary<string, object>()
        };
        var body = this.bus.PayloadConverter.FromObject(message.Payload);

        var headers = propsVisitor?.Invoke(props);
        headers.ForEach(i => props.Headers.Add(i));

        this.bus.MessageLabelHandler.Inject(props, message.Label);

        DiagnosticProps.Store(DiagnosticProps.Names.LastPublishAttemptConnectionString, ConnectionString);
        return (nativeRoute, body, props);
    }

    /// <summary>
    /// The reject.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    /// <param name="requeue">
    /// The requeue.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Reject(RabbitDelivery delivery, bool requeue, CancellationToken cancellationToken)
    {
        this.logger.LogTrace("Rejecting message [{Label}] ({DeliveryTag})", delivery.Label, delivery.Args.DeliveryTag);

        await SafeNativeInvoke(n => n.BasicNackAsync(delivery.Args.DeliveryTag, false, requeue, cancellationToken).AsTask(), cancellationToken);
    }

    /// <summary>
    /// The reply.
    /// </summary>
    /// <param name="message">
    /// The message.
    /// </param>
    /// <param name="replyTo">
    /// The reply to.
    /// </param>
    /// <param name="correlationId">
    /// The correlation id.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task Reply<TReply>(Message<TReply> message, RabbitRoute replyTo, string correlationId, CancellationToken cancellationToken) where TReply : class
    {
        Func<IBasicProperties, IDictionary<string, object>> propsVisitor = p =>
        {
            p.CorrelationId = correlationId;
            return message.Headers;
        };

        await Publish(new RabbitRoute(replyTo.Exchange, replyTo.RoutingKey), message, propsVisitor, cancellationToken);
    }

    /// <summary>
    /// The set qos.
    /// </summary>
    /// <param name="qos">
    /// The qos.
    /// </param>
    /// <param name="cancellationToken"></param>
    public async Task SetQos(QoSParams qos, CancellationToken cancellationToken)
    {
        try
        {
            await SafeNativeInvoke(n => n.BasicQosAsync(qos.PrefetchSize, qos.PrefetchCount, false, cancellationToken: cancellationToken), cancellationToken);
        }
        catch (Exception e)
        {
            this.logger.LogError(e, "Failed to set Qos on channel: {Channel}", ToString());
            throw;
        }
    }

    /// <summary>
    /// The start consuming.
    /// </summary>
    /// <param name="listeningSource">
    /// The listening source.
    /// </param>
    /// <param name="requireAccept">
    /// The require accept.
    /// </param>
    /// <param name="consumer">
    /// The consumer.
    /// </param>
    /// <param name="cancellationToken"></param>
    /// <returns>
    /// The <see cref="string"/>.
    /// </returns>
    public async Task<string> StartConsuming(IListeningSource listeningSource, bool requireAccept, IAsyncBasicConsumer consumer, CancellationToken cancellationToken)
    {
        try
        {
            var consumerTag = string.Empty;

            await SafeNativeInvoke(async n =>
            {
                consumerTag = await n.BasicConsumeAsync(listeningSource.Address, !requireAccept, consumer, cancellationToken: cancellationToken);
            }, cancellationToken);

            return consumerTag;
        }
        catch (Exception e)
        {
            this.logger.LogError(e, "Failed start consuming on channel");
            throw;
        }
    }

    /// <summary>
    /// Unsubscribe consumer from queue via <param name="tag"/> subscription ID.
    /// </summary>
    /// <param name="tag">Subscription ID</param>
    /// <param name="cancellationToken"></param>
    public async Task StopConsuming(string tag, CancellationToken cancellationToken)
    {
        if (IsOpen)
        {
            await SafeNativeInvoke(m => m.BasicCancelAsync(tag, cancellationToken: cancellationToken), cancellationToken);
        }
        else
        {
            this.logger.LogTrace("Channel is closed, skipping canceling consumer");
        }
    }

    /// <summary>
    /// The unpack as.
    /// </summary>
    /// <param name="delivery">
    /// The delivery.
    /// </param>
    /// <returns>
    /// The <see cref="IMessage"/>.
    /// </returns>
    public Message<T> UnpackAs<T>(RabbitDelivery delivery) where T : class
    {
        var payload = this.bus.PayloadConverter.ToObject<T>(delivery.Args.Body.ToArray());
        return new Message<T>(delivery.Label, payload, delivery.Headers);
    }

    public override string ToString()
    {
        return $"RabbitChannel({this.connection.Id};{Channel?.ChannelNumber})";
    }

    private async Task OnChannelShutdown(object sender, ShutdownEventArgs args)
    {
        this.logger.LogTrace("Channel is closed due to '{ReplyText}'", args.ReplyText);
        await Shutdown.Invoke(new RabbitChannelShutdownEventArgs(this, args, args.CancellationToken));
    }

    private async Task SafeNativeInvoke(Func<IChannel, Task> invokeAction, CancellationToken token)
    {
        if (!IsOpen)
        {
            this.logger.LogWarning("Channel is closed, cannot perform action [{ActionName}]", invokeAction.Method.Name);
            return;
        }

        await this.semaphore.ExecuteWithCondition(async () =>
        {
            if (!IsOpen)
            {
                this.logger.LogWarning("Channel is closed, cannot perform action [{ActionName}]", invokeAction.Method.Name);
                return 0;
            }

            try
            {
                this.logger.LogTrace("Performing channel action [{ActionName}]", invokeAction.Method.Name);
                await invokeAction(Channel);
                return 0;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Channel action failed due to {Message}, connection string: [{ConnectionString}]", ex.Message, ConnectionStringHelper.Sanitize(ConnectionString));
                throw;
            }
        }, logger: this.logger, cancellationToken: token);
    }
}
