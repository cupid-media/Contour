using System;
using System.Configuration;
using System.Dynamic;
using System.Linq;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Receiving.Consumers;
using Contour.Serialization;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

/// <summary>
///   Публичный тип, который конфигурирование по секции должно найти по имени
///   при сканировании загруженных сборок.
/// </summary>
public class ConfiguratorTestPayload
{
    public string Value { get; set; }
}

[TestFixture]
[Category("Unit")]
public class KafkaBusConfigurationExtensionsTests
{
    private static IncomingConfiguration CreateReceiverConfig(string type)
    {
        return new IncomingConfiguration
        {
            Label = "cfg-topic",
            GroupId = "cfg-group",
            ConnectionString = "cfg:9092",
            EnableAutoCommit = false,
            ParallelismLevel = 2,
            CommitOffset = 7,
            MaxPendingAcknowledgements = 300,
            RequiresAccept = true,
            React = "myConsumer",
            Type = type
        };
    }

    private static OutgoingConfiguration CreateSenderConfig()
    {
        return new OutgoingConfiguration
        {
            Label = "cfg-out",
            ConnectionString = "cfg:9092"
        };
    }

    /// <summary>Резолвер, отдающий обработчик и запоминающий, о чём его спросили.</summary>
    private sealed class RecordingResolver
    {
        private readonly object consumer;

        public RecordingResolver(object consumer = null)
        {
            this.consumer = consumer;
        }

        public string ResolvedName { get; private set; }

        public Type ResolvedType { get; private set; }

        public IPayloadConverter PayloadConverter { get; set; }

        public object Resolve(string name, Type type)
        {
            if (type == typeof(IPayloadConverter))
            {
                return PayloadConverter;
            }

            ResolvedName = name;
            ResolvedType = type;
            return this.consumer;
        }
    }

    [Test]
    public void Configure_maps_receiver_config_and_registers_typed_consumer()
    {
        var receiverConfig = CreateReceiverConfig(nameof(ConfiguratorTestPayload));
        var section = new KafkaEndpoint { Incoming = new[] { receiverConfig } };
        var consumer = new TestKafkaConsumerOf<ConfiguratorTestPayload>();
        var resolver = new RecordingResolver(consumer);

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, resolver.Resolve);

        resolver.ResolvedName.Should().Be("myConsumer");
        resolver.ResolvedType.Should().Be(typeof(IConsumerOf<ConfiguratorTestPayload>));

        var receiverConfigurator = cfg.ReceiverConfigurators.Should().ContainSingle().Subject;
        receiverConfigurator.Topic.Should().Be("cfg-topic");

        var options = receiverConfigurator.Options;
        options.Label.Name.Should().Be("cfg-topic");
        options.GroupId.Should().Be("cfg-group");
        options.ConnectionString.Should().Be("cfg:9092");
        options.EnableAutoCommit.Should().BeFalse();
        options.ParallelismLevel.Should().Be(2u);
        options.CommitOffset.Should().Be(7);
        options.MaxPendingAcknowledgements.Should().Be(300);
        options.AcceptRequired.Should().BeTrue();
        options.PayloadConverter.Should().NotBeNull("the bus payload converter must be applied");

        var receiver = new FakeKafkaReceiver("cfg-topic");
        options.ReceiverRegistration(receiver);

        receiver.RegisteredTopic.Should().Be("cfg-topic");
        receiver.RegisteredConsumer.Should().BeSameAs(consumer);
        receiver.RegisteredMessageType.Should().Be(typeof(ConfiguratorTestPayload));
    }

    [Test]
    public void Configure_returns_the_same_configurator()
    {
        var cfg = new KafkaBusConfigurator();

        cfg.Configure(new KafkaEndpoint(), (name, type) => null).Should().BeSameAs(cfg);
    }

    [Test]
    public void Configure_prefers_the_resolved_payload_converter()
    {
        var receiverConfig = CreateReceiverConfig(nameof(ConfiguratorTestPayload));
        var section = new KafkaEndpoint { Incoming = new[] { receiverConfig } };
        var resolvedConverter = new PassThroughPayloadConverter();

        var resolver = new RecordingResolver(new TestKafkaConsumerOf<ConfiguratorTestPayload>())
        {
            PayloadConverter = resolvedConverter
        };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, resolver.Resolve);

        cfg.ReceiverConfigurators.Single().Options.PayloadConverter.Should().BeSameAs(resolvedConverter);
    }

    [Test]
    public void Configure_falls_back_to_expando_object_when_type_is_not_set()
    {
        var section = new KafkaEndpoint { Incoming = new[] { CreateReceiverConfig(type: null) } };
        var resolver = new RecordingResolver(new TestKafkaConsumerOf<ExpandoObject>());

        new KafkaBusConfigurator().Configure(section, resolver.Resolve);

        resolver.ResolvedType.Should().Be(typeof(IConsumerOf<ExpandoObject>));
    }

    [Test]
    public void Configure_with_unknown_type_throws_configuration_error()
    {
        var section = new KafkaEndpoint { Incoming = new[] { CreateReceiverConfig("NoSuchTypeAnywhere_42") } };

        var configure = () => new KafkaBusConfigurator().Configure(section, (name, type) => null);

        configure.Should().Throw<ConfigurationErrorsException>();
    }

    [Test]
    public void Configure_maps_sender_config_and_declares_the_route()
    {
        var senderConfig = CreateSenderConfig();
        senderConfig.Key = "outAlias";
        senderConfig.Confirm = false;
        senderConfig.FailoverAttempts = 5;
        senderConfig.Acks = Acks.All;
        senderConfig.CompressionType = CompressionType.Lz4;
        senderConfig.EnableIdempotence = true;
        senderConfig.MessageTimeout = TimeSpan.FromSeconds(45);

        var section = new KafkaEndpoint { Outgoing = new[] { senderConfig } };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => null);

        var senderConfigurator = cfg.SenderConfigurators.Should().ContainSingle().Subject;
        senderConfigurator.Label.Name.Should().Be("cfg-out");

        var options = senderConfigurator.Options;
        options.ConnectionString.Should().Be("cfg:9092");
        options.Key.Should().Be(":outAlias", "an alias is normalised to the label alias form");
        options.ConfirmationRequired.Should().BeFalse();
        options.FailoverAttempts.Should().Be(5);
        options.Acks.Should().Be(Acks.All);
        options.CompressionType.Should().Be(CompressionType.Lz4);
        options.EnableIdempotence.Should().BeTrue();
        options.MessageTimeout.Should().Be(TimeSpan.FromSeconds(45));
        options.PayloadConverter.Should().NotBeNull("the bus payload converter must be applied");
        options.RequiresCallback.Should().BeFalse("request-response is off unless asked for");
    }

    [Test]
    public void Configure_enables_request_response_when_the_section_asks_for_it()
    {
        var senderConfig = CreateSenderConfig();
        senderConfig.RequiresCallback = true;
        senderConfig.RequestTimeout = TimeSpan.FromSeconds(15);

        var section = new KafkaEndpoint { Outgoing = new[] { senderConfig } };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => null);

        var options = cfg.SenderConfigurators.Single().Options;

        options.RequiresCallback.Should().BeTrue();
        options.CallbackTopic.Should().BeNull("the topic is derived from the label when the bus is created");
        options.RequestTimeout.Should().Be(TimeSpan.FromSeconds(15));
    }

    [Test]
    public void Configure_with_a_callback_topic_enables_request_response_implicitly()
    {
        var senderConfig = CreateSenderConfig();
        senderConfig.CallbackLabel = "cfg-out.reply";

        var section = new KafkaEndpoint { Outgoing = new[] { senderConfig } };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => null);

        var options = cfg.SenderConfigurators.Single().Options;

        options.RequiresCallback.Should().BeTrue();
        options.CallbackTopic.Should().Be("cfg-out.reply");
    }

    [Test]
    public void Configure_declares_receivers_and_senders_from_one_section()
    {
        var section = new KafkaEndpoint
        {
            Incoming = new[] { CreateReceiverConfig(nameof(ConfiguratorTestPayload)) },
            Outgoing = new[] { CreateSenderConfig() }
        };

        var resolver = new RecordingResolver(new TestKafkaConsumerOf<ConfiguratorTestPayload>());

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, resolver.Resolve);

        cfg.ReceiverConfigurators.Select(c => c.Topic).Should().Equal("cfg-topic");
        cfg.SenderConfigurators.Select(c => c.Label.Name).Should().Equal("cfg-out");
    }

    [Test]
    public void Configure_applies_endpoint_level_settings_as_defaults()
    {
        var section = new KafkaEndpoint
        {
            Name = "cfg-endpoint",
            ConnectionString = "endpoint:9092",
            GroupId = "endpoint-group",
            ParallelismLevel = 4,
            CommitOffset = 55,
            CommitTimeout = TimeSpan.FromSeconds(11),
            MaxPendingAcknowledgements = 77,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            PartitionAssignmentStrategy = Confluent.Kafka.PartitionAssignmentStrategy.CooperativeSticky,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SslCaPem = "ca",
            Debug = "consumer",
            Incoming = new[] { new IncomingConfiguration { Label = "t", React = "myConsumer", Type = nameof(ConfiguratorTestPayload) } },
            Outgoing = new[] { new OutgoingConfiguration { Label = "out" } }
        };

        var resolver = new RecordingResolver(new TestKafkaConsumerOf<ConfiguratorTestPayload>());

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, resolver.Resolve);

        // Получатель и маршрут не задавали ничего своего — всё пришло с уровня секции.
        var receiver = cfg.ReceiverConfigurators.Single().Options;
        receiver.ConnectionString.Should().Be("endpoint:9092");
        receiver.GroupId.Should().Be("endpoint-group");
        receiver.ParallelismLevel.Should().Be(4u);
        receiver.CommitOffset.Should().Be(55);
        receiver.CommitTimeout.Should().Be(TimeSpan.FromSeconds(11));
        receiver.MaxPendingAcknowledgements.Should().Be(77);
        receiver.AutoOffsetReset.Should().Be(AutoOffsetReset.Earliest);
        receiver.EnableAutoCommit.Should().BeFalse();
        receiver.PartitionAssignmentStrategy.Should().Be(Confluent.Kafka.PartitionAssignmentStrategy.CooperativeSticky);
        receiver.SecurityProtocol.Should().Be(SecurityProtocol.SaslSsl);
        receiver.SslCaPem.Should().Be("ca");
        receiver.Debug.Should().Be("consumer");

        var sender = cfg.SenderConfigurators.Single(c => c.Label.Name == "out").Options;
        sender.ConnectionString.Should().Be("endpoint:9092");
        sender.SecurityProtocol.Should().Be(SecurityProtocol.SaslSsl);
        sender.SslCaPem.Should().Be("ca");
    }

    [Test]
    public void Configure_lets_an_element_override_the_endpoint_defaults()
    {
        var section = new KafkaEndpoint
        {
            ConnectionString = "endpoint:9092",
            GroupId = "endpoint-group",
            Incoming = new[]
            {
                new IncomingConfiguration
                {
                    Label = "own",
                    ConnectionString = "own:9092",
                    GroupId = "own-group",
                    React = "myConsumer",
                    Type = nameof(ConfiguratorTestPayload)
                }
            }
        };

        var resolver = new RecordingResolver(new TestKafkaConsumerOf<ConfiguratorTestPayload>());

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, resolver.Resolve);

        var receiver = cfg.ReceiverConfigurators.Single().Options;
        receiver.ConnectionString.Should().Be("own:9092");
        receiver.GroupId.Should().Be("own-group");
    }

    [Test]
    public void Configure_leaves_settings_absent_from_the_section_untouched()
    {
        // Секция без имени и серверов не должна затирать то, что задано кодом.
        var section = new KafkaEndpoint();

        var cfg = new KafkaBusConfigurator()
            .WithEndpointName("from-code")
            .WithConnectionString("code:9092")
            .WithGroupId("code-group");

        cfg.Configure(section, (name, type) => null);

        cfg.On("probe").Options.ConnectionString.Should().Be("code:9092");
        cfg.ReceiverConfigurators.Single().Options.GroupId.Should().Be("code-group");
    }

    [Test]
    public void Configure_resolves_the_lifecycle_handler_by_name()
    {
        var handler = new RecordingLifecycleHandler();

        var section = new KafkaEndpoint { LifecycleHandler = "myHandler" };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => name == "myHandler" ? handler : null);

        cfg.BusLifecycleHandler.Should().BeSameAs(handler);
    }

    [Test]
    public void Configure_enables_the_default_fault_strategy()
    {
        var section = new KafkaEndpoint { ConnectionString = "endpoint:9092" };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => null);

        cfg.On("probe").Options.FailedDeliveryStrategy.Should().NotBeNull();
    }

    [Test]
    public void Configure_declares_a_catch_all_route_for_dynamic_outgoing()
    {
        var section = new KafkaEndpoint
        {
            ConnectionString = "endpoint:9092",
            Dynamic = new DynamicOutgoing()
        };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => null);

        cfg.SenderConfigurators.Should().ContainSingle().Which.Label.IsAny.Should().BeTrue();
    }

    [Test]
    public void Configure_declares_no_catch_all_route_without_dynamic_outgoing()
    {
        var section = new KafkaEndpoint { ConnectionString = "endpoint:9092" };

        var cfg = new KafkaBusConfigurator();
        cfg.Configure(section, (name, type) => null);

        cfg.SenderConfigurators.Should().BeEmpty();
    }

    private sealed class DynamicOutgoing : Contour.Configuration.IDynamic
    {
        public bool? Outgoing => true;
    }

    private sealed class RecordingLifecycleHandler : IBusLifecycleHandler
    {
        public void OnStarting(IBus bus)
        {
        }

        public void OnStarted(IBus bus)
        {
        }

        public void OnStopping(IBus bus)
        {
        }

        public void OnStopped(IBus bus)
        {
        }
    }

    [Test]
    public void Configure_with_null_arguments_throws()
    {
        var cfg = new KafkaBusConfigurator();
        var section = new KafkaEndpoint();

        var nullSection = () => cfg.Configure(null, (name, type) => null);
        var nullResolverFunc = () => cfg.Configure(section, (DependencyResolverFunc)null);
        var nullResolver = () => cfg.Configure(section, (IDependencyResolver)null);

        nullSection.Should().Throw<ArgumentNullException>();
        nullResolverFunc.Should().Throw<ArgumentNullException>();
        nullResolver.Should().Throw<ArgumentNullException>();
    }
}
