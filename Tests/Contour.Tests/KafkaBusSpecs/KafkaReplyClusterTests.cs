using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Serialization;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.Kafka.Internal;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

/// <summary>
///   Ответы и пересылки из обработчика уходят в тот кластер, из которого прочитано сообщение,
///   а не в кластер, заданный на уровне шины. Исключение — стратегия провалов по умолчанию:
///   как и fault-очередь RabbitMQ, fault-топик пишется в кластер шины.
/// </summary>
[TestFixture]
[Category("Unit")]
public class KafkaReplyClusterTests
{
    private const string BusServers = "bus:9092";

    private const string ReceiverServers = "receiver:9092";

    private const string FaultTopic = "reply-cluster.fault";

    private FakeKafkaBroker broker;

    private RecordingProducerFactory producerFactory;

    [SetUp]
    public void SetUp()
    {
        this.broker = new FakeKafkaBroker();
        this.producerFactory = new RecordingProducerFactory(this.broker.ProducerFactory);
    }

    private Task<IBus> CreateBus(string busServers, Action<KafkaBusConfigurator> configure)
    {
        return BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ConsumerFactory = this.broker.ConsumerFactory;
            cfg.ProducerFactory = this.producerFactory;
            cfg.AutoStart = false;

            cfg.WithEndpointName("reply-cluster")
                .WithConnectionString(busServers)
                .WithGroupId("group")
                .WithPayloadConverter(new PassThroughPayloadConverter());

            configure(cfg);
        });
    }

    private static TestKafkaConsumerOf<string> Forwarding(string topic)
    {
        return new TestKafkaConsumerOf<string> { Callback = context => context.ForwardAsync(topic, context.Message.Payload) };
    }

    private static TestKafkaConsumerOf<string> Failing()
    {
        return new TestKafkaConsumerOf<string> { Callback = _ => throw new InvalidOperationException("boom") };
    }

    [Test]
    public async Task Forward_goes_to_the_cluster_the_message_was_read_from()
    {
        var bus = await CreateBus(BusServers, cfg =>
            cfg.On("in").WithBootstrapServers(ReceiverServers).WithReplies().ReactWith(Forwarding("out")));

        await bus.Start();

        this.broker.Publish("in", Encoding.UTF8.GetBytes("payload"));
        await Wait.Until(() => this.broker.Published("out").Count > 0);

        this.producerFactory.ServersOf("out").Should().Be(ReceiverServers);

        await bus.Stop();
    }

    [Test]
    public async Task Receivers_on_different_clusters_publish_each_to_its_own()
    {
        var bus = await CreateBus(BusServers, cfg =>
        {
            cfg.On("in-a").WithReplies().ReactWith(Forwarding("out-a"));
            cfg.On("in-b").WithBootstrapServers(ReceiverServers).WithReplies().ReactWith(Forwarding("out-b"));
        });

        await bus.Start();

        this.broker.Publish("in-a", Encoding.UTF8.GetBytes("a"));
        this.broker.Publish("in-b", Encoding.UTF8.GetBytes("b"));
        await Wait.Until(() => this.broker.Published("out-a").Count > 0 && this.broker.Published("out-b").Count > 0);

        this.producerFactory.ServersOf("out-a").Should().Be(BusServers);
        this.producerFactory.ServersOf("out-b").Should().Be(ReceiverServers);

        await bus.Stop();
    }

    [Test]
    public async Task Reply_producer_uses_the_security_settings_of_the_receiver()
    {
        var bus = await CreateBus(BusServers, cfg =>
            cfg.On("in").WithBootstrapServers(ReceiverServers).WithSslCaPem("receiver-ca").WithReplies().ReactWith(Forwarding("out")));

        await bus.Start();

        this.broker.Publish("in", Encoding.UTF8.GetBytes("payload"));
        await Wait.Until(() => this.broker.Published("out").Count > 0);

        this.producerFactory.OptionsOf("out").SslCaPem.Should().Be("receiver-ca");

        await bus.Stop();
    }

    [Test]
    public async Task Default_fault_strategy_publishes_to_the_bus_cluster()
    {
        var bus = await CreateBus(BusServers, cfg =>
        {
            cfg.UseDefaultFaultStrategy().WithPayloadConverter(new JsonPayloadConverter());
            cfg.On("in").WithBootstrapServers(ReceiverServers).ReactWith(Failing());
        });

        await bus.Start();

        this.broker.Publish("in", JsonSerializer.SerializeToUtf8Bytes("payload"));
        await Wait.Until(() => this.broker.Published(FaultTopic).Count > 0);

        this.producerFactory.ServersOf(FaultTopic).Should().Be(BusServers);
        this.producerFactory.Count.Should().Be(1, "для кластера получателя продюсер не нужен");

        await bus.Stop();
    }

    [Test]
    public async Task Default_fault_strategy_uses_the_bus_servers_set_after_it()
    {
        var bus = await CreateBus(null, cfg =>
        {
            cfg.UseDefaultFaultStrategy().WithPayloadConverter(new JsonPayloadConverter());
            cfg.On("in").WithBootstrapServers(ReceiverServers).ReactWith(Failing());
            cfg.WithConnectionString(BusServers);
        });

        await bus.Start();

        this.broker.Publish("in", JsonSerializer.SerializeToUtf8Bytes("payload"));
        await Wait.Until(() => this.broker.Published(FaultTopic).Count > 0);

        this.producerFactory.ServersOf(FaultTopic).Should().Be(BusServers);

        await bus.Stop();
    }

    [Test]
    public async Task Default_fault_strategy_requires_bus_level_servers()
    {
        var create = () => CreateBus(null, cfg =>
        {
            cfg.UseDefaultFaultStrategy().WithPayloadConverter(new JsonPayloadConverter());
            cfg.On("in").WithBootstrapServers(ReceiverServers).ReactWith(Failing());
        });

        (await create.Should().ThrowAsync<BusConfigurationException>())
            .Which.Message.Should().Contain(FaultTopic);
    }

    [Test]
    public async Task No_producer_is_created_for_a_receiver_without_replies()
    {
        var bus = await CreateBus(BusServers, cfg => cfg.On("in").ReactWith(new TestKafkaConsumerOf<string>()));

        await bus.Start();

        this.producerFactory.Count.Should().Be(0);

        await bus.Stop();
    }

    [Test]
    public async Task Forward_without_replies_fails_with_a_hint()
    {
        var outcome = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        var bus = await CreateBus(BusServers, cfg => cfg.On("in").ReactWith(new TestKafkaConsumerOf<string>
        {
            Callback = async context =>
            {
                try
                {
                    await context.ForwardAsync("out", context.Message.Payload);
                    outcome.TrySetResult(null);
                }
                catch (Exception ex)
                {
                    outcome.TrySetResult(ex);
                }
            }
        }));

        await bus.Start();

        this.broker.Publish("in", Encoding.UTF8.GetBytes("payload"));

        var error = await outcome.Task.WaitAsync(TimeSpan.FromSeconds(5));

        error.Should().BeOfType<BusConfigurationException>().Which.Message.Should().Contain("WithReplies()");

        await bus.Stop();
    }

    /// <summary>
    ///   Фабрика продюсеров, запоминающая, с какими настройками создан каждый продюсер.
    /// </summary>
    private sealed class RecordingProducerFactory(IKafkaProducerFactory inner) : IKafkaProducerFactory
    {
        private readonly ConcurrentQueue<(FakeKafkaProducer Producer, KafkaSenderOptions Options)> created = new();

        public IProducer<string, byte[]> Create(KafkaSenderOptions options, Action<Error> errorHandler)
        {
            var producer = (FakeKafkaProducer)inner.Create(options, errorHandler);
            this.created.Enqueue((producer, options));
            return producer;
        }

        public KafkaSenderOptions OptionsOf(string topic)
        {
            return this.created.Single(c => c.Producer.Produced.Any(m => m.Topic == topic)).Options;
        }

        public string ServersOf(string topic) => OptionsOf(topic).ConnectionString;

        public int Count => this.created.Count;
    }
}
