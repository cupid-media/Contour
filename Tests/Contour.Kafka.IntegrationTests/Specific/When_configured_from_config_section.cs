using Confluent.Kafka;
using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Serialization;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Specific;

/// <summary>
/// End-to-end check of <see cref="KafkaBusConfigurationExtensions.Configure(KafkaBusConfigurator, IKafkaEndpoint, Contour.Configuration.DependencyResolverFunc)"/>:
/// a whole bus — endpoint settings, a receiver and a send route — described by one section,
/// with the handler resolved through the dependency resolver.
/// </summary>
public class When_configured_from_config_section : KafkaIntegrationTestBase
{
    private TestKafkaConsumer<DummyMessage> consumer = null!;
    private string topic = null!;
    private bool canRoute;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.topic = await CreateTopic();
        this.consumer = new TestKafkaConsumer<DummyMessage>();

        // Всё общее задано на уровне секции; у элементов — только их топики и обработчик.
        var section = new KafkaEndpoint
        {
            Name = DefaultEndpointName,
            ConnectionString = BootstrapServers,
            GroupId = DefaultGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            ParallelismLevel = 1,
            CommitOffset = 1,
            Confirm = true,
            Incoming = [
                new IncomingConfiguration
                {
                    Label = this.topic,
                    React = "dummyConsumer",
                    Type = nameof(DummyMessage)
                }
            ],
            Outgoing = [new OutgoingConfiguration { Label = this.topic }]
        };

        var bus = Track(await BusFactory.CreateKafkaBus(cfg =>
            cfg.Configure(
                section,
                (name, type) => type == typeof(IPayloadConverter) ? PayloadConverter : Resolve(name))));

        this.canRoute = bus.CanRoute(this.topic);

        // Отправка идёт через маршрут, объявленный секцией, а не сырым продюсером.
        await bus.EmitAsync(this.topic, new DummyMessage { Id = 7 });

        await WaitForCondition(() => this.consumer.HandledCount == 1);
    }

    [Test]
    public void Then_receiver_from_the_section_consumes_messages()
    {
        this.consumer.Handled.Should().ContainSingle().Which.Payload.Id.Should().Be(7);
    }

    [Test]
    public void Then_message_is_deserialized_into_the_configured_type()
    {
        this.consumer.Handled.Single().Payload.Should().BeOfType<DummyMessage>();
        this.consumer.Handled.Single().Label.Name.Should().Be(this.topic);
    }

    [Test]
    public void Then_sender_from_the_section_can_route()
    {
        this.canRoute.Should().BeTrue();
    }

    [Test]
    public void Then_endpoint_name_from_the_section_is_used()
    {
        this.consumer.Handled.Single().Headers[Headers.Breadcrumbs].Should().Be(DefaultEndpointName);
    }

    private object Resolve(string name)
    {
        return name == "dummyConsumer"
            ? this.consumer
            : throw new InvalidOperationException($"Unexpected dependency [{name}]");
    }
}
