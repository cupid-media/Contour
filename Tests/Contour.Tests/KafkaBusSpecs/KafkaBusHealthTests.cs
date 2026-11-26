using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

[TestFixture]
[Category("Unit")]
public class KafkaBusHealthTests
{
    private const string Topic = "health-topic";

    private FakeKafkaConsumer consumer;
    private FakeKafkaConsumerFactory factory;
    private TestKafkaConsumerOf<string> messageConsumer;

    [SetUp]
    public void SetUp()
    {
        this.consumer = new FakeKafkaConsumer();
        this.factory = new FakeKafkaConsumerFactory();
        this.factory.Prepare(this.consumer);
        this.messageConsumer = new TestKafkaConsumerOf<string>();
    }

    private Task<IBus> CreateBus()
    {
        return BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ConsumerFactory = this.factory;
            cfg.AutoStart = false;

            cfg.WithConnectionString("fake:9092")
                .WithGroupId("health-group")
                .WithAutoCommit(false)
                .WithPayloadConverter(new PassThroughPayloadConverter());

            cfg.On(Topic).ReactWith(this.messageConsumer);
        });
    }

    [Test]
    public async Task Bus_is_healthy_only_while_working()
    {
        var bus = await CreateBus();

        bus.IsHealthy.Should().BeFalse("it is not started yet");

        await bus.Start();
        bus.IsHealthy.Should().BeTrue("it is started and workers are alive");

        await bus.StopReceiving();
        bus.IsHealthy.Should().BeFalse("receiving is stopped");

        await bus.Stop();
        bus.IsHealthy.Should().BeFalse("it is stopped");
    }

    [Test]
    public async Task Bus_becomes_unhealthy_when_worker_dies_on_fatal_error()
    {
        var bus = await CreateBus();
        await bus.Start();

        bus.IsHealthy.Should().BeTrue();

        // Фатальная ошибка транспорта (не ConsumeException и не отмена) убивает воркера.
        this.consumer.FailNextConsume(new InvalidOperationException("fatal transport error"));

        await Wait.Until(() => !bus.IsHealthy, because: "dead worker must be reported as unhealthy");

        bus.IsReady.Should().BeTrue("bus is formally working, but the health check reveals the dead worker");

        await bus.Stop();
    }

    [Test]
    public async Task Consume_exception_does_not_affect_health()
    {
        var bus = await CreateBus();
        await bus.Start();

        this.consumer.FailNextConsume(new ConsumeException(
            new ConsumeResult<byte[], byte[]>(),
            new Error(ErrorCode.Local_TimedOut)));

        this.consumer.Enqueue(Topic, 1, "still-alive");
        await Wait.Until(() => this.messageConsumer.Handled.Count == 1);

        bus.IsHealthy.Should().BeTrue("recoverable consume errors must not kill the worker");

        await bus.Stop();
    }
}
