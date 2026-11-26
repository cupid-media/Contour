using System;
using System.Linq;
using Confluent.Kafka;
using Contour.Receiving;
using Contour.Serialization;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.Kafka.Internal;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

[TestFixture]
[Category("Unit")]
public class KafkaBusConfiguratorTests
{
    [Test]
    public void On_with_duplicate_topic_throws()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.On("topic");

        var declareDuplicate = () => configurator.On("topic");

        declareDuplicate.Should().Throw<ArgumentException>();
    }

    [Test]
    public void On_with_empty_topic_throws()
    {
        var configurator = new KafkaBusConfigurator();

        var declareEmpty = () => configurator.On(" ");

        declareEmpty.Should().Throw<ArgumentException>();
    }

    [Test]
    public void On_snapshots_defaults_into_independent_receiver_options()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.WithConnectionString("srv-1:9092");
        configurator.WithGroupId("group-1");

        configurator.On("topic-a");
        configurator.On("topic-b");

        // Дефолты, выставленные после On, не должны влиять на уже объявленные ресиверы.
        configurator.WithConnectionString("srv-2:9092");

        var options = configurator.ReceiverConfigurators.Select(c => c.Options).ToArray();

        options.Select(o => o.Label.Name).Should().Equal("topic-a", "topic-b");
        options.Select(o => o.ConnectionString).Should().AllBe("srv-1:9092");
        options.Select(o => o.GroupId).Should().AllBe("group-1");
        options[0].Should().NotBeSameAs(options[1]);
    }

    [Test]
    public void Receiver_settings_override_bus_defaults()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.WithConnectionString("srv:9092")
            .WithGroupId("shared-group")
            .WithParallelismLevel(2);

        configurator.On("topic-a");
        configurator.On("topic-b")
            .WithGroupId("own-group")
            .WithParallelismLevel(8);

        var byTopic = configurator.ReceiverConfigurators.ToDictionary(c => c.Topic, c => c.Options);

        byTopic["topic-a"].GroupId.Should().Be("shared-group");
        byTopic["topic-a"].ParallelismLevel.Should().Be(2u);
        byTopic["topic-b"].GroupId.Should().Be("own-group");
        byTopic["topic-b"].ParallelismLevel.Should().Be(8u);
        byTopic["topic-b"].ConnectionString.Should().Be("srv:9092", "servers are inherited from the bus defaults");
    }

    [Test]
    public void ReactWith_registers_consumer_through_receiver_registration()
    {
        var configurator = new KafkaBusConfigurator();
        var consumer = new TestKafkaConsumerOf<string>();

        configurator.On("topic").ReactWith(consumer);

        var receiver = new FakeKafkaReceiver();
        configurator.ReceiverConfigurators.Single().Options.ReceiverRegistration(receiver);

        receiver.RegisteredTopic.Should().Be("topic");
        receiver.RegisteredConsumer.Should().BeSameAs(consumer);
        receiver.RegisteredMessageType.Should().Be(typeof(string));
    }

    [Test]
    public void PostConfigure_pushes_bus_payload_converter_into_receivers_that_have_none()
    {
        var configurator = new KafkaBusConfigurator();
        var ownConverter = new PassThroughPayloadConverter();

        configurator.On("inherits");
        configurator.On("overrides").WithPayloadConverter(ownConverter);

        // Конвертер шины задаётся уже после объявления топиков — порядок не должен иметь значения.
        var busConverter = new PassThroughPayloadConverter();
        configurator.WithPayloadConverter(busConverter);
        configurator.PostConfigure();

        var byTopic = configurator.ReceiverConfigurators.ToDictionary(c => c.Topic, c => c.Options);

        byTopic["inherits"].PayloadConverter.Should().BeSameAs(busConverter);
        byTopic["overrides"].PayloadConverter.Should().BeSameAs(ownConverter);
    }

    [Test]
    public void Default_payload_converter_is_json()
    {
        new KafkaBusConfigurator().PayloadConverter.Should().BeOfType<JsonPayloadConverter>();
    }

    [Test]
    public void Auto_start_is_enabled_by_default()
    {
        new KafkaBusConfigurator().AutoStart.Should().BeTrue();
    }

    [Test]
    public void Topic_name_is_taken_from_the_message_type()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.WithConnectionString("srv:9092").WithGroupId("g");

        configurator.On<LabelledPayload>();
        configurator.Route<RoutedPayload>();

        configurator.ReceiverConfigurators.Single().Topic.Should().Be("labelled.payload");
        configurator.SenderConfigurators.Single().Label.Name.Should().Be("routed.payload");
    }

    [Test]
    public void A_message_type_without_the_attribute_is_rejected()
    {
        var configurator = new KafkaBusConfigurator();

        // Имя топика берётся только из [Message]: по имени типа он не выводится.
        configurator.Invoking(c => c.On<PlainPayload>())
            .Should().Throw<Contour.Configuration.BusConfigurationException>()
            .WithMessage("*MessageAttribute*");
    }

    [Test]
    public void Mixed_case_topic_names_are_rejected()
    {
        var configurator = new KafkaBusConfigurator();

        configurator.Invoking(c => c.On("My.Label")).Should().Throw<ArgumentException>()
            .WithMessage("*lower case*");
        configurator.Invoking(c => c.Route("My.Label")).Should().Throw<ArgumentException>();
        configurator.Invoking(c => c.Route("route").WithCallbackTopic("My.Reply")).Should().Throw<ArgumentException>();
    }

    [Test]
    public void Fetch_settings_are_applied_to_receivers()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.WithConnectionString("srv:9092")
            .WithGroupId("g")
            .WithFetch(queuedMinMessages: 250, maxPartitionFetchBytes: 2048);

        var options = configurator.On("topic").Options;

        options.QueuedMinMessages.Should().Be(250);
        options.MaxPartitionFetchBytes.Should().Be(2048);
    }

    [Test]
    public void Fetch_settings_keep_the_consumer_configuration_valid()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.WithConnectionString("srv:9092")
            .WithGroupId("g")
            .WithFetch(queuedMinMessages: 250, maxPartitionFetchBytes: 2048);

        var options = configurator.On("topic").Options;

        // librdkafka отвергает конфигурацию с неизвестным свойством целиком, поэтому настройки
        // fetch должны раскладываться в имена, которые он действительно знает.
        options.Invoking(o => new ConsumerBuilder<Ignore, byte[]>(o.BuildConsumerConfig()).Build().Dispose())
            .Should().NotThrow();
    }

    [Test]
    public void Fetch_settings_reject_non_positive_values()
    {
        var configurator = new KafkaBusConfigurator();

        configurator.Invoking(c => c.WithFetch(0)).Should().Throw<ArgumentOutOfRangeException>();
        configurator.Invoking(c => c.WithFetch(1, 0)).Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void Bootstrap_servers_provider_wins_over_the_static_value()
    {
        var configurator = new KafkaBusConfigurator();
        configurator.WithConnectionString("static:9092")
            .WithGroupId("g")
            .WithBootstrapServersProvider(new FixedServersProvider("provided:9092"));

        configurator.On("topic");
        configurator.Route("out");
        configurator.PostConfigure();

        configurator.ReceiverConfigurators.Single().Options.ConnectionString.Should().Be("provided:9092");
        configurator.SenderConfigurators.Single().Options.ConnectionString.Should().Be("provided:9092");
    }

    private sealed class FixedServersProvider : Contour.Configuration.IConnectionStringProvider
    {
        private readonly string servers;

        public FixedServersProvider(string servers) => this.servers = servers;

        public string GetConnectionString(MessageLabel messageLabel) => this.servers;
    }

    [Contour.Message("labelled.payload")]
    private sealed class LabelledPayload
    {
    }

    [Contour.Message("routed.payload")]
    private sealed class RoutedPayload
    {
    }

    private sealed class PlainPayload
    {
    }

    [Test]
    public void Receiver_defaults_disable_autocommit()
    {
        KafkaBusDefaults.DefaultReceiverOptions.EnableAutoCommit.Should().BeFalse();
        KafkaBusDefaults.DefaultReceiverOptions.BuildConsumerConfig().EnableAutoCommit.Should().BeFalse();
    }

    [Test]
    public void BuildConsumerConfig_leaves_client_offset_store_untouched_without_autocommit()
    {
        var options = new KafkaReceiverOptions { ConnectionString = "srv:9092", GroupId = "g", EnableAutoCommit = false };

        var config = options.BuildConsumerConfig();

        config.EnableAutoCommit.Should().BeFalse();
        config.EnableAutoOffsetStore.Should().BeNull();
    }

    [Test]
    public void BuildConsumerConfig_disables_client_offset_store_with_autocommit()
    {
        var options = new KafkaReceiverOptions { ConnectionString = "srv:9092", GroupId = "g", EnableAutoCommit = true };

        var config = options.BuildConsumerConfig();

        config.EnableAutoCommit.Should().BeTrue();
        config.EnableAutoOffsetStore.Should().BeFalse("only the tracker's safe positions may reach the broker");
    }

    [Test]
    public void ReceiverOptions_copy_constructor_copies_confluent_and_library_settings()
    {
        Action<IReceiver> registration = _ => { };

        var source = new KafkaReceiverOptions
        {
            Label = MessageLabel.From("t"),
            ConnectionString = "srv:9092",
            GroupId = "g",
            EnableAutoCommit = false,
            ParallelismLevel = 4,
            CommitOffset = 42,
            CommitTimeout = TimeSpan.FromSeconds(9),
            PayloadConverter = new PassThroughPayloadConverter(),
            ReceiverRegistration = registration
        };

        var copy = new KafkaReceiverOptions(source);
        source.ConnectionString = "changed:9092";

        copy.Label.Name.Should().Be("t");
        copy.ConnectionString.Should().Be("srv:9092");
        copy.GroupId.Should().Be("g");
        copy.EnableAutoCommit.Should().BeFalse();
        copy.ParallelismLevel.Should().Be(4u);
        copy.CommitOffset.Should().Be(42);
        copy.CommitTimeout.Should().Be(TimeSpan.FromSeconds(9));
        copy.PayloadConverter.Should().BeSameAs(source.PayloadConverter);
        copy.ReceiverRegistration.Should().BeSameAs(registration);
    }
}
