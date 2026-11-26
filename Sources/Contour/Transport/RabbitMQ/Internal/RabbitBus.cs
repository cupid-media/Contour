using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Sending;
using Contour.Transport.RabbitMQ.Configuration;

namespace Contour.Transport.RabbitMQ.Internal;

internal class RabbitBus : AbstractBus<RabbitBusConfiguration>
{
    private readonly IConnectionPool<IRabbitConnection> connectionPool;

    /// <inheritdoc />
    public override string Transport => BusTransports.Rmq;

    protected override ILogger Logger { get; } = LogManager.GetLogger<RabbitBus>();

    protected override IReadOnlyCollection<IReceiverConfiguration> ReceiverConfigurations => Configuration.ReceiverConfigurations;

    protected override IReadOnlyCollection<ISenderConfiguration> SenderConfigurations => Configuration.SenderConfigurations;

    public RabbitBus(RabbitBusConfiguration configuration)
        : base(configuration)
    {
        this.connectionPool = configuration.ConnectionPool ?? new RabbitConnectionPool(this);
    }

    protected override void RegisterComponents()
    {
        var senderConfigurations = Configuration.SenderConfigurations.ToList();
        var receiverConfigurations = Configuration.ReceiverConfigurations.ToList();

        Logger.LogTrace(
            """
            Configuring [{Name}] with endpoint [{Endpoint}]:
            OutgoingConfiguration:
            	{OutgoingConfiguration}
            IncomingConfiguration:
            	{IncomingConfiguration}
            """,
            GetType().Name,
            Endpoint,
            string.Join("\r\n\t", senderConfigurations.Select(s => $"[{s.Label}]\t=>\t{s.ConnectionString}")),
            string.Join("\r\n\t", receiverConfigurations.Select(r => $"[{r.Label}]\t=>\t{r.ConnectionString}")));

        foreach (var sender in senderConfigurations)
        {
            RegisterSender(sender);
        }

        foreach (var receiver in receiverConfigurations)
        {
            RegisterReceiver(receiver);
        }
    }

    protected override Task ReleaseResources()
    {
        return this.connectionPool.Drop();
    }

    protected override void ValidateComponents()
    {
        var receivers = ComponentTracker.AllOf<RabbitReceiver>().ToList();
        var multipleReceiversOnSameQueue = receivers
            .SelectMany(r => r.ReceivingSources.Select(s => (r.Configuration.Label, s.BrokerUrl, s.ListeningSource.Address)))
            .GroupBy(x => (x.BrokerUrl, x.Address))
            .Where(x => x.Count() > 1)
            .ToList();

        if (multipleReceiversOnSameQueue.Any())
        {
            var labelsOnQueue = multipleReceiversOnSameQueue.Select(s => $"{s.Key.BrokerUrl}-{s.Key.Address} - [{string.Join(",", s)}]");

            throw new BusConfigurationException($"Multiple receivers are configured on same queue: {string.Join(", ", labelsOnQueue)}");
        }
    }

    /// <summary>
    /// Registers a receiver using the <paramref name="configuration"/>
    /// </summary>
    /// <param name="configuration">
    /// Receiver configuration
    /// </param>
    /// <param name="isCallback">
    /// Denotes if a receiver should handle the callback messages
    /// </param>
    /// <returns>
    /// The <see cref="RabbitReceiver"/>.
    /// </returns>
    public RabbitReceiver RegisterReceiver(RabbitReceiverOptions configuration, bool isCallback = false)
    {
        Logger.LogTrace("Registering a new receiver of [{Label}] with connection string [{ConnectionString}]", configuration.Label, configuration.ConnectionString);

        var receiver = new RabbitReceiver(this, configuration, this.connectionPool);
        if (!isCallback)
        {
            ComponentTracker.Register(receiver);
        }

        Logger.LogTrace("A receiver of [{Label}] with connection string [{ConnectionString}] registered successfully", configuration.Label, configuration.ConnectionString);
        return receiver;
    }

    /// <summary>
    /// Registers a sender using <paramref name="configuration"/>
    /// </summary>
    /// <param name="configuration">
    /// Sender configuration
    /// </param>
    /// <returns>
    /// The <see cref="RabbitSender"/>.
    /// </returns>
    private void RegisterSender(RabbitSenderOptions configuration)
    {
        Logger.LogTrace("Registering a new sender of [{Label}] with connection string [{ConnectionString}]", configuration.Label, configuration.ConnectionString);

        var sender = new RabbitSender(this, configuration, this.connectionPool);
        ComponentTracker.Register(sender);

        Logger.LogTrace("A sender of [{Label}] with connection string [{ConnectionString}] registered successfully", configuration.Label, configuration.ConnectionString);
    }
}
