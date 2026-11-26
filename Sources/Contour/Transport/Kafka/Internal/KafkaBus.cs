using System.Collections.Generic;
using System.Linq;
using Contour.Receiving;
using Contour.Sending;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal class KafkaBus(KafkaBusConfiguration configuration) : AbstractBus<KafkaBusConfiguration>(configuration)
{
    public override string Transport => BusTransports.Kafka;

    protected override ILogger Logger { get; } = LogManager.GetLogger<KafkaBus>();

    protected override IReadOnlyCollection<IReceiverConfiguration> ReceiverConfigurations
        => Configuration.ReceiverConfigurations;

    protected override IReadOnlyCollection<ISenderConfiguration> SenderConfigurations
        => Configuration.SenderConfigurations;

    private readonly Dictionary<string, KafkaSender> replySenders = new();

    internal KafkaSender GetReplySender(string connectionString)
    {
        return connectionString != null && this.replySenders.TryGetValue(connectionString, out var sender) ? sender : null;
    }

    internal KafkaSender GetFaultSender()
    {
        var faultTopic = KafkaTopic.FaultOf(Endpoint.Address);

        return ComponentTracker.AllOf<KafkaSender>().FirstOrDefault(s => s.CanRoute(faultTopic));
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

    public KafkaReceiver RegisterReceiver(KafkaReceiverOptions configuration, bool isCallback = false)
    {
        Logger.LogTrace("Registering a new receiver of [{Label}] with connection string [{ConnectionString}]", configuration.Label, configuration.ConnectionString);

        var receiver = new KafkaReceiver(this, configuration, Configuration.ConsumerFactory);
        if (!isCallback)
        {
            ComponentTracker.Register(receiver);
        }

        if (!isCallback && configuration.Replies && !this.replySenders.ContainsKey(configuration.ConnectionString))
        {
            this.replySenders[configuration.ConnectionString] = RegisterReplySender(configuration);
        }

        Logger.LogTrace("A receiver of [{Label}] with connection string [{ConnectionString}] registered successfully", configuration.Label, configuration.ConnectionString);
        return receiver;
    }

    private KafkaSender RegisterSender(KafkaSenderOptions configuration)
    {
        Logger.LogTrace("Registering a new sender of [{Label}] with connection string [{ConnectionString}]", configuration.Label, configuration.ConnectionString);

        var sender = new KafkaSender(this, configuration, Configuration.ProducerFactory);
        ComponentTracker.Register(sender);

        Logger.LogTrace("A sender of [{Label}] with connection string [{ConnectionString}] registered successfully", configuration.Label, configuration.ConnectionString);
        return sender;
    }

    private KafkaSender RegisterReplySender(KafkaReceiverOptions receiver)
    {
        var defaults = Configuration.SenderDefaults;

        var options = defaults with
        {
            Label = MessageLabel.Any,
            RequiresCallback = false,
            ConnectionString = receiver.ConnectionString,
            BootstrapServersProvider = null,
            SecurityProtocol = receiver.SecurityProtocol ?? defaults.SecurityProtocol,
            SslCaPem = receiver.SslCaPem ?? defaults.SslCaPem,
            SslCertificatePem = receiver.SslCertificatePem ?? defaults.SslCertificatePem,
            SslKeyPem = receiver.SslKeyPem ?? defaults.SslKeyPem,
            EnableSslCertificateVerification = receiver.EnableSslCertificateVerification ?? defaults.EnableSslCertificateVerification
        };

        options.PayloadConverter ??= Configuration.Serializer;

        return RegisterSender(options);
    }
}
