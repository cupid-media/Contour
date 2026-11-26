using System;
using System.Configuration;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Helpers;
using Contour.Receiving.Consumers;
using Contour.Sending;

namespace Contour.Transport.RabbitMQ.Configuration;

/// <summary>
///   Экстеншен для конфигурирования RmqBusConfigurator для переданных настроек IRabbitEndpoint
/// </summary>
public static class RabbitBusConfigurationExtensions
{
    /// <summary>
    /// Конфигурирует клиента шины сообщений.
    /// </summary>
    /// <returns>
    /// Конфигуратор клиента шины, после применения к нему всех настроек.
    /// </returns>
    public static RabbitBusConfigurator Configure(this RabbitBusConfigurator cfg, IRabbitEndpoint endpoint, DependencyResolverFunc dependencyResolverFunc)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(dependencyResolverFunc);

        var dependencyResolver = new LambdaDependencyResolver(dependencyResolverFunc);
        ConfigureEndpoint(cfg, endpoint, dependencyResolver);

        foreach (var outgoingElement in endpoint.Outgoing)
        {
            ConfigureOutgoing(cfg, outgoingElement);
        }

        var typeResolver = new MessageTypeResolver();
        foreach (var incomingElement in endpoint.Incoming)
        {
            ConfigureIncoming(cfg, incomingElement, dependencyResolver, typeResolver);
        }

        return cfg;
    }

    private static void ConfigureEndpoint(RabbitBusConfigurator cfg, IRabbitEndpoint endpoint, IDependencyResolver dependencyResolver)
    {
        cfg.WithEndpointName(endpoint.Name)
            .WithConnectionString(endpoint.ConnectionString)
            .WithExcludedHeaders(endpoint.ExcludedHeaders)
            .WithConnectionStringProvider(GetConnectionStringProvider(endpoint, dependencyResolver));

        if (endpoint.ReuseConnection.HasValue)
        {
            cfg.WithReuseConnection(endpoint.ReuseConnection.Value);
        }

        if (endpoint.ParallelismLevel.HasValue)
        {
            cfg.WithParallelismLevel(endpoint.ParallelismLevel.Value);
        }

        if (endpoint.FaultQueueTtl.HasValue)
        {
            cfg.WithFaultQueueTtl(endpoint.FaultQueueTtl.Value);
        }

        if (endpoint.FaultQueueLimit.HasValue)
        {
            cfg.WithFaultQueueLimit(endpoint.FaultQueueLimit.Value);
        }

        if (endpoint.QueueLimit.HasValue)
        {
            cfg.WithQueueLimit(endpoint.QueueLimit.Value);
        }

        if (endpoint.QueueMaxLengthBytes.HasValue)
        {
            cfg.WithQueueMaxLengthBytes(endpoint.QueueMaxLengthBytes.Value);
        }

        if (endpoint.Qos?.PrefetchCount != null || endpoint.Qos?.PrefetchSize != null)
        {
            cfg.WithQoS(endpoint.Qos.PrefetchCount ?? 0, endpoint.Qos.PrefetchSize ?? 0);
        }

        if (!string.IsNullOrWhiteSpace(endpoint.LifecycleHandler))
        {
            cfg.WithBusLifecycleHandler(dependencyResolver.Resolve<IBusLifecycleHandler>(endpoint.LifecycleHandler));
        }

        if (endpoint.Dynamic?.Outgoing == true)
        {
            cfg.Route(MessageLabel.Any).ConfiguredWith((builder, _) => new ValueTask<IRouteResolver>(new DynamicRouteResolver(builder)));
        }

        cfg.UseDefaultFaultStrategy();
    }

    private static IConnectionStringProvider GetConnectionStringProvider(IRabbitEndpoint endpoint, IDependencyResolver dependencyResolver)
    {
        if (!string.IsNullOrEmpty(endpoint.ConnectionStringProvider))
        {
            return dependencyResolver.Resolve<IConnectionStringProvider>(endpoint.ConnectionStringProvider);
        }

        return null;
    }

    private static void ConfigureOutgoing(RabbitBusConfigurator cfg, IOutgoing outgoingElement)
    {
        var configurator = cfg.Route(outgoingElement.Label)
            .WithAlias(outgoingElement.Key)
            .WithConfirmation(outgoingElement.Confirm, outgoingElement.ConfirmTimeout)
            .Persistently(outgoingElement.Persist)
            .WithTtl(outgoingElement.Ttl)
            .Delayed(outgoingElement.Delayed)
            .ReuseConnection(outgoingElement.ReuseConnection);

        if (!string.IsNullOrEmpty(outgoingElement.ConnectionString))
        {
            configurator.WithConnectionString(outgoingElement.ConnectionString);
        }

        if (outgoingElement.Timeout != null)
        {
            configurator.WithRequestTimeout(outgoingElement.Timeout);
        }

        if (outgoingElement.CallbackEndpoint.Default)
        {
            configurator.WithDefaultCallbackEndpoint();
        }
    }

    private static void ConfigureIncoming(RabbitBusConfigurator cfg, IIncoming incomingElement, IDependencyResolver dependencyResolver, MessageTypeResolver typeResolver)
    {
        var configurator = cfg.On(incomingElement.Label)
            .RequiresAccept(incomingElement.RequiresAccept)
            .Delayed(incomingElement.Delayed);

        if (!string.IsNullOrEmpty(incomingElement.ConnectionString))
        {
            configurator.WithConnectionString(incomingElement.ConnectionString);
        }

        if (incomingElement.Qos != null)
        {
            configurator
                .WithQoS(new QoSParams(incomingElement.Qos.PrefetchCount ?? configurator.Options.QoS.PrefetchCount, incomingElement.Qos?.PrefetchSize ?? configurator.Options.QoS.PrefetchSize));
        }

        if (incomingElement.ParallelismLevel.HasValue)
        {
            configurator.WithParallelismLevel(incomingElement.ParallelismLevel.Value);
        }

        if (incomingElement.QueueLimit.HasValue)
        {
            configurator.WithQueueLimit(incomingElement.QueueLimit.Value);
        }

        if (incomingElement.QueueMaxLengthBytes.HasValue)
        {
            configurator.WithQueueMaxLengthBytes(incomingElement.QueueMaxLengthBytes.Value);
        }

        if (incomingElement.RequiresAccept)
        {
            configurator.RequiresAccept();
        }

        if (incomingElement.ReuseConnection.HasValue)
        {
            configurator.ReuseConnection(incomingElement.ReuseConnection.Value);
        }

        var messageType = typeof(ExpandoObject);
        if (!string.IsNullOrWhiteSpace(incomingElement.Type))
        {
            messageType = typeResolver.Resolve(incomingElement.Type);
        }

        var consumer = BuildConsumer(() => ResolveConsumer(dependencyResolver, incomingElement.React, messageType), messageType, incomingElement.Lifestyle);
        RegisterConsumer(configurator, messageType, consumer);
    }

    private static IConsumer BuildConsumer(Func<IConsumer> consumerFactory, Type messageType, Lifestyle? lifestyle)
    {
        Type consumerType;
        switch (lifestyle)
        {
            case null:
            case Lifestyle.Normal:
                return consumerFactory();
            case Lifestyle.Lazy:
                consumerType = typeof(LazyConsumerOf<>).MakeGenericType(messageType);
                return Activator.CreateInstance(consumerType, consumerFactory) as IConsumer;
            case Lifestyle.Delegated:
                consumerType = typeof(FactoryConsumerOf<>).MakeGenericType(messageType);
                return Activator.CreateInstance(consumerType, consumerFactory) as IConsumer;
            default:
                throw new ConfigurationErrorsException("Unknown or unsupported consumer lifestyle : [{0}].".FormatEx(lifestyle));
        }
    }

    private static void RegisterConsumer(RabbitReceiverConfigurator configurator, Type messageType, IConsumer consumer)
    {
        // Нужно из Type сделать TModel
        typeof(RabbitBusConfigurationExtensions).GetMethod(nameof(RegisterConsumerWrapper), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(messageType)
            .Invoke(null, [configurator, consumer]);
    }

    private static void RegisterConsumerWrapper<TModel>(RabbitReceiverConfigurator configurator, IConsumer<TModel> consumer) where TModel: class
    {
        configurator.ReactWith(consumer);
    }

    private static IConsumer ResolveConsumer(IDependencyResolver dependencyResolver, string name, Type messageType)
    {
        return dependencyResolver.Resolve<IConsumer>(name, typeof(IConsumer<>).MakeGenericType(messageType));
    }
}
