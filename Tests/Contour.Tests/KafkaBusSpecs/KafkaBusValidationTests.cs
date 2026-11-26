using System;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

/// <summary>
///   Конфигурация проверяется в <see cref="KafkaBusConfigurator.Validate"/>, который вызывает
///   <see cref="BusFactory.CreateKafkaBus"/> — до создания шины.
/// </summary>
[TestFixture]
[Category("Unit")]
public class KafkaBusValidationTests
{
    private static void ApplyValidDefaults(KafkaBusConfigurator cfg)
    {
        cfg.ConsumerFactory = new FakeKafkaConsumerFactory();
        cfg.AutoStart = false;

        cfg.WithConnectionString("fake:9092")
            .WithGroupId("validation-group")
            .WithPayloadConverter(new PassThroughPayloadConverter());
    }

    private static async Task AssertCreationFails(Action<KafkaBusConfigurator> configure, string expectedMessagePart)
    {
        var create = () => BusFactory.CreateKafkaBus(cfg =>
        {
            ApplyValidDefaults(cfg);
            configure(cfg);
        });

        (await create.Should().ThrowAsync<BusConfigurationException>())
            .WithMessage($"*{expectedMessagePart}*");
    }

    [Test]
    public async Task Creation_fails_when_nothing_is_configured()
    {
        await AssertCreationFails(_ => { }, "No senders and receivers are registered");
    }

    [Test]
    public async Task Creation_fails_when_bootstrap_servers_are_missing()
    {
        await AssertCreationFails(
            cfg =>
            {
                cfg.WithConnectionString(null);
                cfg.On("topic").ReactWith(new TestKafkaConsumerOf<string>());
            },
            "ConnectionString");
    }

    [Test]
    public async Task Creation_fails_when_group_id_is_missing()
    {
        await AssertCreationFails(
            cfg =>
            {
                cfg.WithGroupId(null);
                cfg.On("topic").ReactWith(new TestKafkaConsumerOf<string>());
            },
            "GroupId");
    }

    [Test]
    public async Task Creation_fails_when_no_consumer_is_registered_for_topic()
    {
        await AssertCreationFails(cfg => cfg.On("topic-without-consumer"), "No consumer is registered");
    }

    [Test]
    public async Task Creation_fails_when_payload_converter_is_missing()
    {
        await AssertCreationFails(
            cfg =>
            {
                cfg.WithPayloadConverter(null);
                cfg.On("topic").ReactWith(new TestKafkaConsumerOf<string>());
            },
            "PayloadConverter");
    }

    [Test]
    public void Commit_offset_must_be_positive()
    {
        var configurator = new KafkaBusConfigurator();

        var setZero = () => configurator.WithCommitOffset(0);

        setZero.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void Commit_timeout_must_be_positive()
    {
        var configurator = new KafkaBusConfigurator();

        var setZero = () => configurator.WithCommitTimeout(TimeSpan.Zero);

        setZero.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void Parallelism_level_must_be_greater_than_zero()
    {
        var configurator = new KafkaBusConfigurator();

        var setZero = () => configurator.WithParallelismLevel(0);

        setZero.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public void Receiver_level_ranges_are_validated_too()
    {
        var receiver = new KafkaBusConfigurator().On("topic");

        receiver.Invoking(r => r.WithParallelismLevel(0)).Should().Throw<ArgumentOutOfRangeException>();
        receiver.Invoking(r => r.WithCommitOffset(0)).Should().Throw<ArgumentOutOfRangeException>();
        receiver.Invoking(r => r.WithCommitTimeout(TimeSpan.Zero)).Should().Throw<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Valid_configuration_produces_a_working_bus()
    {
        var bus = await BusFactory.CreateKafkaBus(cfg =>
        {
            ApplyValidDefaults(cfg);
            cfg.AutoStart = true;
            cfg.On("topic").ReactWith(new TestKafkaConsumerOf<string>());
        });

        bus.State.Should().Be(BusState.Working);

        await bus.Stop();
    }

    [Test]
    public async Task Bus_is_not_started_when_auto_start_is_disabled()
    {
        var bus = await BusFactory.CreateKafkaBus(cfg =>
        {
            ApplyValidDefaults(cfg);
            cfg.On("topic").ReactWith(new TestKafkaConsumerOf<string>());
        });

        bus.State.Should().Be(BusState.NotInitialized);
        bus.IsReady.Should().BeFalse();
    }
}
