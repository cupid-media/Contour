using System;
using System.Configuration;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Contour.Configuration;
using Contour.Receiving.Consumers;
using Contour.Serialization;

namespace Contour.Transport.Kafka.Configuration;

public static class KafkaBusConfigurationExtensions
{
    public static KafkaBusConfigurator Configure(this KafkaBusConfigurator cfg, IKafkaEndpoint section, DependencyResolverFunc dependencyResolverFunc)
    {
        ArgumentNullException.ThrowIfNull(dependencyResolverFunc);

        return Configure(cfg, section, new LambdaDependencyResolver(dependencyResolverFunc));
    }

    public static KafkaBusConfigurator Configure(this KafkaBusConfigurator cfg, IKafkaEndpoint section, IDependencyResolver dependencyResolver)
    {
        ArgumentNullException.ThrowIfNull(cfg);
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(dependencyResolver);

        // Настройки уровня секции применяются первыми: получатели и маршруты снимают
        // копию дефолтов в момент объявления.
        ConfigureEndpoint(cfg, section, dependencyResolver);

        foreach (var senderElement in section.Outgoing)
        {
            ConfigureSender(cfg, senderElement, dependencyResolver);
        }

        var typeResolver = new MessageTypeResolver();
        foreach (var receiverElement in section.Incoming.Where(r => r.ParallelismLevel != 0))
        {
            ConfigureReceiver(cfg, receiverElement, dependencyResolver, typeResolver);
        }

        return cfg;
    }

    private static void ConfigureEndpoint(
        KafkaBusConfigurator cfg,
        IKafkaEndpoint section,
        IDependencyResolver dependencyResolver)
    {
        if (!string.IsNullOrWhiteSpace(section.Name))
        {
            cfg.WithEndpointName(section.Name);
        }

        if (!string.IsNullOrWhiteSpace(section.ConnectionString))
        {
            cfg.WithConnectionString(section.ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(section.GroupId))
        {
            cfg.WithGroupId(section.GroupId);
        }

        if (section.ExcludedHeaders != null)
        {
            cfg.WithExcludedHeaders(section.ExcludedHeaders);
        }

        if (section.ParallelismLevel.HasValue)
        {
            cfg.WithParallelismLevel(section.ParallelismLevel.Value);
        }

        if (section.RequestTimeout.HasValue)
        {
            cfg.WithRequestTimeout(section.RequestTimeout);
        }

        if (section.FailoverAttempts.HasValue)
        {
            cfg.WithFailoverAttempts(section.FailoverAttempts.Value);
        }

        if (section.Confirm.HasValue || section.ConfirmTimeout.HasValue)
        {
            cfg.WithConfirmation(section.Confirm ?? true, section.ConfirmTimeout);
        }

        if (section.CommitOffset.HasValue)
        {
            cfg.WithCommitOffset(section.CommitOffset.Value);
        }

        if (section.CommitTimeout.HasValue)
        {
            cfg.WithCommitTimeout(section.CommitTimeout.Value);
        }

        if (section.MaxPendingAcknowledgements.HasValue)
        {
            cfg.WithMaxPendingAcknowledgements(section.MaxPendingAcknowledgements.Value);
        }

        if (section.AutoOffsetReset.HasValue)
        {
            cfg.WithAutoOffsetReset(section.AutoOffsetReset.Value);
        }

        if (section.EnableAutoCommit.HasValue)
        {
            cfg.WithAutoCommit(section.EnableAutoCommit.Value);
        }

        if (section.PartitionAssignmentStrategy.HasValue)
        {
            cfg.WithPartitionAssignmentStrategy(section.PartitionAssignmentStrategy.Value);
        }

        if (section.SecurityProtocol.HasValue)
        {
            cfg.WithSecurityProtocol(section.SecurityProtocol.Value);
        }

        if (!string.IsNullOrWhiteSpace(section.SslCaPem))
        {
            cfg.WithSslCaPem(section.SslCaPem);
        }

        if (!string.IsNullOrWhiteSpace(section.SslCertificatePem))
        {
            cfg.WithSslCertificatePem(section.SslCertificatePem);
        }

        if (!string.IsNullOrWhiteSpace(section.SslKeyPem))
        {
            cfg.WithSslKeyPem(section.SslKeyPem);
        }

        if (section.EnableSslCertificateVerification.HasValue)
        {
            cfg.WithSslCertificateVerification(section.EnableSslCertificateVerification.Value);
        }

        if (!string.IsNullOrWhiteSpace(section.Debug))
        {
            cfg.WithDebug(section.Debug);
        }

        if (!string.IsNullOrWhiteSpace(section.LifecycleHandler))
        {
            cfg.WithBusLifecycleHandler(dependencyResolver.Resolve<IBusLifecycleHandler>(section.LifecycleHandler));
        }

        // Маршрут на любую метку: позволяет отправлять в топик, не объявленный в секции.
        if (section.Dynamic?.Outgoing == true)
        {
            cfg.Route(MessageLabel.Any);
        }

        cfg.UseDefaultFaultStrategy();
    }

    private static void ConfigureReceiver(
        KafkaBusConfigurator cfg,
        IIncoming receiverElement,
        IDependencyResolver dependencyResolver,
        MessageTypeResolver typeResolver)
    {
        // У каждого топика могут быть свои серверы и группа; незаданное берётся
        // из дефолтов уровня секции и не должно затираться.
        var configurator = cfg.On(receiverElement.Label);

        if (!string.IsNullOrWhiteSpace(receiverElement.ConnectionString))
        {
            configurator.WithBootstrapServers(receiverElement.ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(receiverElement.GroupId))
        {
            configurator.WithGroupId(receiverElement.GroupId);
        }

        if (receiverElement.CommitOffset.HasValue)
        {
            configurator.WithCommitOffset(receiverElement.CommitOffset.Value);
        }

        if (receiverElement.CommitTimeout.HasValue)
        {
            configurator.WithCommitTimeout(receiverElement.CommitTimeout.Value);
        }

        if (receiverElement.MaxPendingAcknowledgements.HasValue)
        {
            configurator.WithMaxPendingAcknowledgements(receiverElement.MaxPendingAcknowledgements.Value);
        }

        if (receiverElement.AutoOffsetReset.HasValue)
        {
            configurator.WithAutoOffsetReset(receiverElement.AutoOffsetReset.Value);
        }

        if (receiverElement.EnableAutoCommit.HasValue)
        {
            configurator.WithAutoCommit(receiverElement.EnableAutoCommit.Value);
        }

        if (receiverElement.PartitionAssignmentStrategy.HasValue)
        {
            configurator.WithPartitionAssignmentStrategy(receiverElement.PartitionAssignmentStrategy.Value);
        }

        if (receiverElement.SecurityProtocol.HasValue)
        {
            configurator.WithSecurityProtocol(receiverElement.SecurityProtocol.Value);
        }

        if (!string.IsNullOrWhiteSpace(receiverElement.SslCaPem))
        {
            configurator.WithSslCaPem(receiverElement.SslCaPem);
        }

        if (!string.IsNullOrWhiteSpace(receiverElement.SslCertificatePem))
        {
            configurator.WithSslCertificatePem(receiverElement.SslCertificatePem);
        }

        if (!string.IsNullOrWhiteSpace(receiverElement.SslKeyPem))
        {
            configurator.WithSslKeyPem(receiverElement.SslKeyPem);
        }

        if (receiverElement.EnableSslCertificateVerification.HasValue)
        {
            configurator.WithSslCertificateVerification(receiverElement.EnableSslCertificateVerification.Value);
        }

        if (receiverElement.RequiresAccept.HasValue)
        {
            configurator.RequiresAccept(receiverElement.RequiresAccept.Value);
        }

        if (receiverElement.Replies.HasValue)
        {
            configurator.WithReplies(receiverElement.Replies.Value);
        }

        if (receiverElement.ParallelismLevel.HasValue)
        {
            configurator.WithParallelismLevel(receiverElement.ParallelismLevel.Value);
        }

        if (!string.IsNullOrWhiteSpace(receiverElement.Debug))
        {
            configurator.WithDebug(receiverElement.Debug);
        }

        configurator.WithPayloadConverter(ResolvePayloadConverter(dependencyResolver) ?? cfg.PayloadConverter);

        var messageType = typeof(ExpandoObject);
        if (!string.IsNullOrWhiteSpace(receiverElement.Type))
        {
            messageType = typeResolver.Resolve(receiverElement.Type);
        }

        var consumer = BuildConsumer(
            () => ResolveConsumer(dependencyResolver, receiverElement.React, messageType),
            messageType,
            receiverElement.Lifestyle);

        RegisterConsumer(configurator, messageType, consumer);
    }

    private static void ConfigureSender(
        KafkaBusConfigurator cfg,
        IOutgoing senderElement,
        IDependencyResolver dependencyResolver)
    {
        var configurator = cfg.Route(senderElement.Label);

        if (!string.IsNullOrWhiteSpace(senderElement.ConnectionString))
        {
            configurator.WithBootstrapServers(senderElement.ConnectionString);
        }

        if (!string.IsNullOrWhiteSpace(senderElement.Key))
        {
            configurator.WithKey(senderElement.Key);
        }

        if (senderElement.Confirm.HasValue || senderElement.ConfirmTimeout.HasValue)
        {
            configurator.WithConfirmation(senderElement.Confirm ?? true, senderElement.ConfirmTimeout);
        }

        if (senderElement.FailoverAttempts.HasValue)
        {
            configurator.WithFailoverAttempts(senderElement.FailoverAttempts.Value);
        }

        if (senderElement.FlushTimeout.HasValue)
        {
            configurator.WithFlushTimeout(senderElement.FlushTimeout.Value);
        }

        if (senderElement.RequestTimeout.HasValue)
        {
            configurator.WithRequestTimeout(senderElement.RequestTimeout);
        }

        // Выделенный топик ответов сам по себе включает запрос-ответ.
        if (!string.IsNullOrWhiteSpace(senderElement.CallbackLabel))
        {
            configurator.WithCallbackTopic(senderElement.CallbackLabel);
        }
        else if (senderElement.RequiresCallback)
        {
            configurator.WithDefaultCallbackEndpoint();
        }

        if (senderElement.Acks.HasValue)
        {
            configurator.WithAcks(senderElement.Acks.Value);
        }

        if (senderElement.CompressionType.HasValue)
        {
            configurator.WithCompression(senderElement.CompressionType.Value);
        }

        if (senderElement.EnableIdempotence.HasValue)
        {
            configurator.WithIdempotence(senderElement.EnableIdempotence.Value);
        }

        if (senderElement.Linger.HasValue)
        {
            configurator.WithLinger(senderElement.Linger.Value);
        }

        if (senderElement.MessageTimeout.HasValue)
        {
            configurator.WithMessageTimeout(senderElement.MessageTimeout.Value);
        }

        if (senderElement.SecurityProtocol.HasValue)
        {
            configurator.WithSecurityProtocol(senderElement.SecurityProtocol.Value);
        }

        if (!string.IsNullOrWhiteSpace(senderElement.SslCaPem))
        {
            configurator.WithSslCaPem(senderElement.SslCaPem);
        }

        if (!string.IsNullOrWhiteSpace(senderElement.SslCertificatePem))
        {
            configurator.WithSslCertificatePem(senderElement.SslCertificatePem);
        }

        if (!string.IsNullOrWhiteSpace(senderElement.SslKeyPem))
        {
            configurator.WithSslKeyPem(senderElement.SslKeyPem);
        }

        if (senderElement.EnableSslCertificateVerification.HasValue)
        {
            configurator.WithSslCertificateVerification(senderElement.EnableSslCertificateVerification.Value);
        }

        if (!string.IsNullOrWhiteSpace(senderElement.Debug))
        {
            configurator.WithDebug(senderElement.Debug);
        }

        configurator.WithPayloadConverter(ResolvePayloadConverter(dependencyResolver) ?? cfg.PayloadConverter);
    }

    private static object BuildConsumer(Func<object> consumerFactory, Type messageType, Lifestyle? lifestyle)
    {
        switch (lifestyle)
        {
            case null:
            case Lifestyle.Normal:
                return consumerFactory();

            case Lifestyle.Lazy:
                return Activator.CreateInstance(
                    typeof(LazyConsumerOf<>).MakeGenericType(messageType), consumerFactory);

            case Lifestyle.Delegated:
                return Activator.CreateInstance(
                    typeof(FactoryConsumerOf<>).MakeGenericType(messageType), consumerFactory);

            default:
                throw new ConfigurationErrorsException($"Unknown or unsupported consumer lifestyle : [{lifestyle}].");
        }
    }

    private static object ResolveConsumer(IDependencyResolver dependencyResolver, string name, Type messageType)
    {
        return dependencyResolver.Resolve<object>(name, typeof(IConsumerOf<>).MakeGenericType(messageType));
    }

    private static IPayloadConverter ResolvePayloadConverter(IDependencyResolver dependencyResolver)
    {
        return dependencyResolver.Resolve<IPayloadConverter>(null);
    }

    private static void RegisterConsumer(KafkaReceiverConfigurator configurator, Type messageType, object consumer)
    {
        typeof(KafkaBusConfigurationExtensions).GetMethod(nameof(RegisterConsumerWrapper), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(messageType)
            .Invoke(null, [configurator, consumer]);
    }

    private static void RegisterConsumerWrapper<TModel>(KafkaReceiverConfigurator configurator, IConsumerOf<TModel> consumer)
        where TModel : class
    {
        configurator.ReactWith(consumer);
    }
}
