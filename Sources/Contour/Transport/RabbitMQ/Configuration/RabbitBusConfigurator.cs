using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Contour.Configuration;
using Contour.Helpers;
using Contour.Receiving;
using Contour.Sending;
using Contour.Serialization;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Transport.RabbitMQ.Configuration;

public class RabbitBusConfigurator
{
    private static readonly ILogger Logger = LogManager.GetLogger<RabbitBusConfigurator>();

    public bool AutoStart { get; set; } = true;

    public IBusLifecycleHandler BusLifecycleHandler { get; private set; }

    public IPayloadConverter PayloadConverter { get; private set; }

    public IMessageLabelHandler MessageLabelHandler { get; private set; }

    internal IConnectionPool<IRabbitConnection> ConnectionPool { get; set; }

    public List<RabbitSenderConfiguration> SenderConfigurators { get; } = [];

    public List<RabbitReceiverConfigurator> ReceiverConfigurators { get; } = [];

    private MessageLabelResolver MessageLabelResolver { get; } = new();

    private readonly RabbitSenderOptions senderDefaults;

    private readonly RabbitReceiverOptions receiverDefaults;

    private Endpoint endpoint;

    internal RabbitBusConfigurator()
    {
        this.endpoint = RabbitBusDefaults.DefaultEndpoint;
        this.senderDefaults = RabbitBusDefaults.DefaultSendingOptions;
        this.receiverDefaults = RabbitBusDefaults.DefaultReceiverOptions;
        MessageLabelHandler = new DefaultRabbitMessageLabelHandler();
        PayloadConverter = new JsonPayloadConverter();
    }

    public RabbitBusConfigurator WithEndpointName(string name)
    {
        this.endpoint = new Endpoint(name);
        return this;
    }

    public RabbitBusConfigurator WithConnectionStringProvider(IConnectionStringProvider provider)
    {
        this.senderDefaults.ConnectionStringProvider = provider;
        this.senderDefaults.CallbackOptions.ConnectionStringProvider = provider;
        this.receiverDefaults.ConnectionStringProvider = provider;
        return this;
    }

    public RabbitBusConfigurator WithBusLifecycleHandler(IBusLifecycleHandler handler)
    {
        BusLifecycleHandler = handler;
        return this;
    }

    public RabbitBusConfigurator WithPayloadConverter(IPayloadConverter converter)
    {
        PayloadConverter = converter;
        return this;
    }

    public RabbitBusConfigurator WithIncomingMessageHeaderStorage(IIncomingMessageHeaderStorage storage)
    {
        this.senderDefaults.IncomingMessageHeaderStorage = storage;
        this.receiverDefaults.IncomingMessageHeaderStorage = storage;
        return this;
    }

    public RabbitBusConfigurator WithConnectionString(string connectionString)
    {
        this.senderDefaults.ConnectionString = connectionString;
        this.senderDefaults.CallbackOptions.ConnectionString = connectionString;
        this.receiverDefaults.ConnectionString = connectionString;

        return this;
    }

    public RabbitBusConfigurator WithReuseConnection(bool reuseConnection = true)
    {
        this.senderDefaults.ReuseConnection = reuseConnection;
        this.receiverDefaults.ReuseConnection = reuseConnection;
        return this;
    }

    public RabbitBusConfigurator WithFailoverAttempts(int failoverAttempts)
    {
        if (failoverAttempts < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(failoverAttempts), "Failover attempts must be non-negative");
        }

        this.senderDefaults.FailoverAttempts = failoverAttempts;
        return this;
    }

    public RabbitBusConfigurator WithExcludedHeaders(IReadOnlyCollection<string> excludedHeaders)
    {
        this.receiverDefaults.ExcludedHeaders = excludedHeaders;
        return this;
    }

    public RabbitBusConfigurator WithParallelismLevel(uint parallelismLevel)
    {
        if (parallelismLevel <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(parallelismLevel), "Parallelism level must be greater than zero");
        }

        this.receiverDefaults.ParallelismLevel = parallelismLevel;
        return this;
    }

    public RabbitBusConfigurator WithFaultQueueTtl(TimeSpan faultQueueTtl)
    {
        if (faultQueueTtl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(faultQueueTtl), "Fault queue TTL must be positive");
        }

        this.receiverDefaults.FaultQueueTtl = faultQueueTtl;
        return this;
    }

    public RabbitBusConfigurator WithFaultQueueLimit(int faultQueueLimit)
    {
        if (faultQueueLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(faultQueueLimit), "Fault queue limit must be positive");
        }

        this.receiverDefaults.FaultQueueLimit = faultQueueLimit;
        return this;
    }

    public RabbitBusConfigurator WithQueueLimit(int queueLimit)
    {
        if (queueLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(queueLimit), "Queue limit must be positive");
        }

        this.receiverDefaults.QueueLimit = queueLimit;
        return this;
    }

    public RabbitBusConfigurator WithQueueMaxLengthBytes(int queueMaxLengthBytes)
    {
        if (queueMaxLengthBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(queueMaxLengthBytes), "Queue max length bytes must be positive");
        }

        this.receiverDefaults.QueueMaxLengthBytes = queueMaxLengthBytes;
        return this;
    }

    public RabbitBusConfigurator WithQoS(QoSParams qos)
    {
        this.receiverDefaults.QoS = qos;
        return this;
    }

    public RabbitBusConfigurator WithQoS(ushort prefetchCount, uint prefetchSize = 0)
    {
        this.receiverDefaults.QoS = new QoSParams(prefetchCount, prefetchSize);
        return this;
    }

    public RabbitBusConfigurator UseDefaultFaultStrategy()
    {
        RabbitBusDefaults.ConfigureDefaultFault(this, this.receiverDefaults);
        return this;
    }

    internal void PostConfigure()
    {
        foreach (var receiverConfigurator in ReceiverConfigurators)
        {
            receiverConfigurator.Options.IncomingMessageHeaderStorage.RegisterExcludedHeaders(this.receiverDefaults.ExcludedHeaders);
        }
    }

    internal IBus Create()
    {
        return new RabbitBus(new RabbitBusConfiguration
        {
            Endpoint = this.endpoint,
            MessageLabelHandler = MessageLabelHandler,
            LifecycleHandler = BusLifecycleHandler,
            Serializer = PayloadConverter,
            ReceiverConfigurations = ReceiverConfigurators.Select(c => c.Options).ToList(),
            SenderConfigurations = SenderConfigurators.Select(c => c.Options).ToList(),
            ConnectionPool = ConnectionPool
        });
    }

    public RabbitBusConfigurator WithRouteResolverBuilder(Func<RabbitRouteResolverBuilderParams, CancellationToken, ValueTask<IRouteResolver>> routeResolverBuilder, bool recreateEndpointOnFailure = false)
    {
        this.senderDefaults.RouteResolverBuilder = routeResolverBuilder;
        this.senderDefaults.RecreateRouteOnFailure = recreateEndpointOnFailure;
        return this;
    }

    public RabbitBusConfigurator WithEndpointBuilder(Func<RabbitSubscriptionEndpointBuilderParams, CancellationToken, Task<ISubscriptionEndpoint>> endpointBuilder, bool recreateEndpointOnFailure = false)
    {
        this.receiverDefaults.EndpointBuilder = endpointBuilder;
        this.receiverDefaults.RecreateEndpointOnFailure = recreateEndpointOnFailure;
        return this;
    }

    public RabbitBusConfigurator OnFailed(IFailedDeliveryStrategy failedDeliveryStrategy)
    {
        this.receiverDefaults.FailedDeliveryStrategy = failedDeliveryStrategy;
        return this;
    }

    public RabbitBusConfigurator OnFailed(Func<IFailedConsumingContext, Task> failedDeliveryHandler)
    {
        return OnFailed(new LambdaFailedDeliveryStrategy(failedDeliveryHandler));
    }

    public RabbitBusConfigurator OnUnhandled(IUnhandledDeliveryStrategy unhandledDeliveryStrategy)
    {
        this.receiverDefaults.UnhandledDeliveryStrategy = unhandledDeliveryStrategy;
        return this;
    }

    public RabbitBusConfigurator OnUnhandled(Func<IUnhandledConsumingContext, Task> unhandledDeliveryHandler)
    {
        return OnUnhandled(new LambdaUnhandledDeliveryStrategy(unhandledDeliveryHandler));
    }

    private bool HasRegisteredConsumerFor(MessageLabel label)
    {
        return ReceiverConfigurators.Any(c => c.Options.Label.Equals(label));
    }

    private bool HasRegisteredProducerFor(MessageLabel label)
    {
        return SenderConfigurators.Any(c => c.Label.Equals(label));
    }

    public RabbitReceiverConfigurator On<TModel>() where TModel : class
    {
        return On(MessageLabelResolver.ResolveFrom<TModel>());
    }

    public RabbitReceiverConfigurator On(string label)
    {
        return On(MessageLabel.From(label));
    }

    public RabbitReceiverConfigurator On(MessageLabel label)
    {
        if (HasRegisteredConsumerFor(label))
        {
            throw new ArgumentException($"Receiver for label [{label}] is already registered", nameof(label));
        }

        var configuration = new RabbitReceiverConfigurator(label, this.receiverDefaults);
        ReceiverConfigurators.Add(configuration);
        return configuration;
    }

    public RabbitSenderConfiguration Route(string label)
    {
        return Route(label.ToMessageLabel());
    }

    public RabbitSenderConfiguration Route(MessageLabel label)
    {
        if (HasRegisteredProducerFor(label))
        {
            throw new ArgumentException($"Sender for label [{label}] already registered.", nameof(label));
        }

        var configuration = new RabbitSenderConfiguration(label, this.senderDefaults);
        SenderConfigurators.Add(configuration);
        return configuration;
    }

    public RabbitSenderConfiguration Route<T>() where T : class
    {
        return Route(MessageLabelResolver.ResolveFrom<T>());
    }

    public virtual void Validate()
    {
        Logger.LogTrace("Вызван метод для валидации конфигурации. Aдрес конечной точки - [{EndpointAddress}], получаемые сообщения - [{ReceiverLabels}], отправляемые сообщения - [{SenderLabels}]",
            this.endpoint.Address,
            ReceiverConfigurators != null ? string.Join(";", ReceiverConfigurators.Select(x => x.Options.Label)) : "null",
            SenderConfigurators != null ? string.Join(";", SenderConfigurators.Select(x => x.Label)) : "null");

        if (PayloadConverter == null)
        {
            throw new BusConfigurationException("PayloadConverter is not set.");
        }

        if (this.endpoint.Address == null)
        {
            throw new BusConfigurationException("Не задано название компонента (Endpoint).");
        }

        if (!ReceiverConfigurators.Any() && !SenderConfigurators.Any(c => c.Label.Name is not (SystemQueues.Unhandled or SystemQueues.Fault)))
        {
            throw new BusConfigurationException("No senders and receivers are registered.");
        }

        SenderConfigurators.ForEach(c => c.Validate());
        ReceiverConfigurators.ForEach(c => c.Validate());
    }
}
