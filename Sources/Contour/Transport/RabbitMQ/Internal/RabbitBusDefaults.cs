using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Topology;

namespace Contour.Transport.RabbitMQ.Internal;

internal static class RabbitBusDefaults
{
    private const int FaultMessageTtlDays = 21;

    public static readonly Endpoint DefaultEndpoint = new(Assembly.GetEntryAssembly()?.GetName().Name);

    private static MessageHeaderStorage defaultMessageHeaderStorage;

    public static RabbitReceiverOptions DefaultReceiverOptions => new()
    {
        AcceptRequired = false,
        ParallelismLevel = 1,
        EndpointBuilder = SubscriptionEndpointBuilder,
        QoS = new QoSParams(50, 0),
        ReuseConnection = true,
        IncomingMessageHeaderStorage = GetDefaultMessageHeaderStorage()
    };

    public static RabbitSenderOptions DefaultSendingOptions => new()
    {
        ConfirmationRequired = false,
        Persistently = false,
        RequestTimeout = TimeSpan.FromSeconds(30),
        Ttl = null,
        RouteResolverBuilder = RouteResolverBuilder,
        ReuseConnection = true,
        ProducerSelectorBuilder = new DefaultProducerSelectorBuilder(),
        FailoverAttempts = 7,
        MaxRetryDelay = 30,
        InactivityResetDelay = 120,
        IncomingMessageHeaderStorage = GetDefaultMessageHeaderStorage(),
        CallbackOptions = DefaultReceiverOptions with
        {
            Label = MessageLabel.Any,
            ParallelismLevel = 1,
            QoS = new QoSParams(0, 0),
            RecreateEndpointOnFailure = true
        }
    };

    private static MessageHeaderStorage GetDefaultMessageHeaderStorage()
    {
        if (defaultMessageHeaderStorage != null)
        {
            return defaultMessageHeaderStorage;
        }

        var blockedHeaders = new List<string>
        {
            Headers.Expires,
            Headers.MessageLabel,
            Headers.Persist,
            Headers.QueueMessageTtl,
            Headers.ReplyRoute,
            Headers.Timeout,
            Headers.Ttl,
            Headers.QueueMaxLength,
            Headers.QueueMaxLengthBytes,
            Headers.Delay
        };
        defaultMessageHeaderStorage = new MessageHeaderStorage(blockedHeaders);
        if (Headers.GlobalStorage == null)
        {
            // hack to give access to headers if needed
            Headers.GlobalStorage = defaultMessageHeaderStorage;
        }

        return defaultMessageHeaderStorage;
    }

    public static void ConfigureDefaultFault(RabbitBusConfigurator configurator, RabbitReceiverOptions receiverOptions)
    {
        Func<RabbitRouteResolverBuilderParams, CancellationToken, ValueTask<IRouteResolver>> faultRouteResolverBuilder = async (b, token) =>
        {
            var messageTtl = receiverOptions.FaultQueueTtl ?? TimeSpan.FromDays(FaultMessageTtlDays);
            var name = b.Endpoint.Address + ".Fault";
            var exchange = await b.Topology.Declare(Exchange.Named(name).Durable.Topic, token);
            var queueBuilder = Queue.Named(name).Durable().WithTtl(messageTtl);

            if (receiverOptions.FaultQueueLimit.HasValue)
            {
                queueBuilder = queueBuilder.WithLimit(receiverOptions.FaultQueueLimit.Value);
            }
            var queue = await b.Topology.Declare(queueBuilder, token);
            await b.Topology.Bind(exchange, queue, cancellationToken: token);
            return exchange;
        };

        configurator.Route(SystemQueues.Unhandled).Persistently().ConfiguredWith(faultRouteResolverBuilder).ReuseConnection();
        configurator.Route(SystemQueues.Fault).Persistently().ConfiguredWith(faultRouteResolverBuilder).ReuseConnection();

        configurator.OnUnhandled(
            async d =>
            {
                await d.ForwardAsync(SystemQueues.Unhandled, d.BuildFaultMessage());
                await d.AcceptAsync();
            });

        configurator.OnFailed(
            async d =>
            {
                await d.ForwardAsync(SystemQueues.Fault, d.BuildFaultMessage());
                await d.AcceptAsync();
            });
    }

    public static async ValueTask<IRouteResolver> RouteResolverBuilder(RabbitRouteResolverBuilderParams builderParams, CancellationToken token)
    {
        var exchangeBuilder = Exchange.Named(builderParams.Options.Label.Name).Durable;
        exchangeBuilder = builderParams.Options.Delayed
            ? exchangeBuilder.DelayedFanout
            : exchangeBuilder.Fanout;

        var exchange = await builderParams.Topology.Declare(exchangeBuilder, token);

        return new StaticRouteResolver(exchange);
    }

    public static async Task<ISubscriptionEndpoint> SubscriptionEndpointBuilder(RabbitSubscriptionEndpointBuilderParams builderParams, CancellationToken token)
    {
        var label = builderParams.ReceiverOptions.Label.Name;

        var queueName = builderParams.Endpoint.Address + "." + label;

        var queueBuilder = Queue.Named(queueName).Durable();
        if (builderParams.ReceiverOptions.QueueLimit.HasValue)
        {
            queueBuilder.WithLimit(builderParams.ReceiverOptions.QueueLimit.Value);
        }

        if (builderParams.ReceiverOptions.QueueMaxLengthBytes.HasValue)
        {
            queueBuilder.WithMaxLengthBytes(builderParams.ReceiverOptions.QueueMaxLengthBytes.Value);
        }

        var queue = await builderParams.Topology.Declare(queueBuilder, token);
        var exchangeBuilder = Exchange.Named(label).Durable;
        exchangeBuilder = builderParams.ReceiverOptions.Delayed
            ? exchangeBuilder.DelayedFanout
            : exchangeBuilder.Fanout;

        var exchange = await builderParams.Topology.Declare(exchangeBuilder, token);
        await builderParams.Topology.Bind(exchange, queue, cancellationToken: token);

        return new SubscriptionEndpoint(queue, exchange);
    }
}
