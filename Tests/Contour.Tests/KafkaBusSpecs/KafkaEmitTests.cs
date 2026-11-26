using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Sending;
using Contour.Sending.Exceptions;
using Contour.Tests.Mocks;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.KafkaBusSpecs;

[TestFixture]
[Category("Unit")]
public class KafkaEmitTests
{
    private const string Topic = "emit-topic";

    private FakeKafkaProducerFactory producerFactory;

    [SetUp]
    public void SetUp()
    {
        this.producerFactory = new FakeKafkaProducerFactory();
    }

    private FakeKafkaProducer Producer => this.producerFactory.Created.Single();

    private Task<IBus> CreateBus(Action<Transport.Kafka.Configuration.KafkaBusConfigurator> configure = null)
    {
        return BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ProducerFactory = this.producerFactory;
            cfg.ConsumerFactory = new FakeKafkaConsumerFactory();
            cfg.AutoStart = false;

            cfg.WithEndpointName("emit-endpoint")
                .WithConnectionString("fake:9092")
                .WithPayloadConverter(new PassThroughPayloadConverter());

            // Настройки применяются до объявления маршрута: маршрут снимает копию дефолтов.
            configure?.Invoke(cfg);

            cfg.Route(Topic);
        });
    }

    [Test]
    public async Task Emitted_message_is_produced_to_the_route_topic()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload");

        var produced = Producer.Produced.Should().ContainSingle().Subject;
        produced.Topic.Should().Be(Topic);
        produced.Payload.Should().Be("payload");

        await bus.Stop();
    }

    [Test]
    public async Task Emit_applies_contour_headers()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload");

        var headers = Producer.Produced.Single().Headers;

        headers.Should().ContainKey(Headers.CorrelationId);
        headers.Should().ContainKey(Headers.SentTimestamp);
        headers[Headers.Breadcrumbs].Should().Be("emit-endpoint");

        await bus.Stop();
    }

    [Test]
    public async Task Emit_passes_additional_headers_through()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload", new PublishingOptions
        {
            AdditionalHeaders = new() { ["x-tenant"] = "acme" },
            Ttl = TimeSpan.FromSeconds(30)
        });

        var headers = Producer.Produced.Single().Headers;

        headers["x-tenant"].Should().Be("acme");
        headers[Headers.Ttl].Should().Be("00:00:30", "TimeSpan is encoded with the invariant culture");

        await bus.Stop();
    }

    [Test]
    public async Task Emit_carries_the_trace_context_of_the_current_activity()
    {
        var bus = await CreateBus();
        await bus.Start();

        var activity = new Activity("emit");
        activity.SetIdFormat(ActivityIdFormat.W3C);
        activity.Start();

        try
        {
            await bus.EmitAsync(Topic, "payload");
        }
        finally
        {
            activity.Stop();
        }

        var headers = Producer.Produced.Single().Headers;

        headers[Headers.TraceParent].Should().Be(
            $"00-{activity.TraceId}-{activity.SpanId}-{(byte)activity.ActivityTraceFlags:x2}");

        await bus.Stop();
    }

    [Test]
    public async Task Emit_outside_of_a_trace_sends_no_trace_context()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload");

        var headers = Producer.Produced.Single().Headers;

        headers.Should().NotContainKey(Headers.TraceParent);
        headers.Should().NotContainKey(Headers.TraceState);

        await bus.Stop();
    }

    [Test]
    public async Task Emit_sends_the_key_as_the_message_key()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload", new PublishingOptions { Key = "[\"a\",\"b\"]" });

        Producer.Produced.Single().Key.Should().Be("[\"a\",\"b\"]");

        await bus.Stop();
    }

    [Test]
    public async Task Emit_without_a_key_produces_a_message_without_a_key()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload");

        Producer.Produced.Single().Key.Should().BeNull("librdkafka picks the partition itself, as before");

        await bus.Stop();
    }

    [Test]
    public async Task Key_is_kept_on_failover_attempts()
    {
        var bus = await CreateBus(cfg => cfg.WithFailoverAttempts(2));
        await bus.Start();

        Producer.DeliveryError = new Error(ErrorCode.Local_MsgTimedOut, "delivery timed out");

        var emit = () => bus.EmitAsync(Topic, "doomed", new PublishingOptions { Key = "user-1" });

        await emit.Should().ThrowAsync<FailoverException>();
        Producer.Produced.Should().HaveCount(2).And.OnlyContain(m => m.Key == "user-1");

        await bus.Stop();
    }

    [Test]
    public async Task Unsupported_options_are_reported_once_per_option()
    {
        var logs = new CapturingLoggerFactory();
        LogManager.Initialize(logs);

        try
        {
            var bus = await CreateBus();
            await bus.Start();

            for (var i = 0; i < 3; i++)
            {
                await bus.EmitAsync(Topic, "payload", new PublishingOptions
                {
                    Ttl = TimeSpan.FromSeconds(30),
                    Delay = TimeSpan.FromSeconds(5),
                    Persistently = true,
                    Key = "supported"
                });
            }

            await bus.Stop();
        }
        finally
        {
            LogManager.Initialize(null);
        }

        logs.Warnings.Where(w => w.StartsWith("Option ["))
            .Should().BeEquivalentTo(
                [
                    $"Option [Delay] is set, but the sender of [{Topic}] does not support it: the option has no effect",
                    $"Option [Ttl] is set, but the sender of [{Topic}] does not support it: the option has no effect",
                    $"Option [Persistently] is set, but the sender of [{Topic}] does not support it: the option has no effect"
                ]);
    }

    [Test]
    public async Task Emit_waits_for_the_delivery_report_when_confirmation_is_required()
    {
        var bus = await CreateBus();
        await bus.Start();

        Producer.HoldReports = true;

        var emit = bus.EmitAsync(Topic, "slow");
        await Task.Delay(200);

        emit.IsCompleted.Should().BeFalse("the delivery report has not arrived yet");

        Producer.ReleaseReports();
        await emit.WaitAsync(TimeSpan.FromSeconds(5));

        await bus.Stop();
    }

    [Test]
    public async Task Emit_does_not_wait_for_the_report_when_confirmation_is_disabled()
    {
        var bus = await CreateBus(cfg => cfg.WithConfirmation(false));
        await bus.Start();

        Producer.HoldReports = true;

        await bus.EmitAsync(Topic, "fire-and-forget").WaitAsync(TimeSpan.FromSeconds(5));

        Producer.Produced.Should().ContainSingle();

        await bus.Stop();
    }

    [Test]
    public async Task Emit_fails_after_exhausting_failover_attempts_on_delivery_error()
    {
        var bus = await CreateBus(cfg => cfg.WithFailoverAttempts(2));
        await bus.Start();

        Producer.DeliveryError = new Error(ErrorCode.Local_MsgTimedOut, "delivery timed out");

        var emit = () => bus.EmitAsync(Topic, "doomed");

        await emit.Should().ThrowAsync<FailoverException>();
        Producer.Produced.Should().HaveCount(2, "every failover attempt produces once");

        await bus.Stop();
    }

    [Test]
    public async Task Emit_to_an_unrouted_label_is_rejected()
    {
        var bus = await CreateBus();
        await bus.Start();

        var emit = () => bus.EmitAsync("unknown-topic", "payload");

        await emit.Should().ThrowAsync<Contour.Configuration.BusConfigurationException>();

        await bus.Stop();
    }

    [Test]
    public async Task Request_is_not_supported()
    {
        var bus = await CreateBus();
        await bus.Start();

        var request = () => bus.RequestAsync<string, string>(Topic, "payload");

        await request.Should().ThrowAsync<NotSupportedException>();

        await bus.Stop();
    }

    [Test]
    public async Task Bus_routes_and_resolves_configured_labels()
    {
        var bus = await CreateBus(cfg => cfg.Route("aliased-topic").WithKey("alias"));
        await bus.Start();

        bus.CanRoute(Topic).Should().BeTrue();
        bus.CanRoute("unknown-topic").Should().BeFalse();
        bus.Resolve(":alias").Name.Should().Be("aliased-topic");

        await bus.Stop();
    }

    [Test]
    public async Task Stopping_the_bus_flushes_the_producer()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload");
        await bus.Stop();

        Producer.FlushCount.Should().BeGreaterThan(0, "buffered messages must be written on shutdown");
        Producer.IsDisposed.Should().BeTrue();
    }

    [Test]
    public async Task Fatal_broker_error_makes_the_sender_unhealthy()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "payload");
        Producer.ErrorHandler(new Error(ErrorCode.Local_Fatal, "fatal error", isFatal: true));

        // Продюсер на маршрут один, подменить его некем: пока он не пересобран,
        // отправитель не отправит ничего, и здоровым считаться не должен.
        bus.IsHealthy.Should().BeFalse();

        await bus.Stop();
    }

    [Test]
    public async Task Producer_is_rebuilt_after_a_fatal_broker_error()
    {
        var bus = await CreateBus();
        await bus.Start();

        await bus.EmitAsync(Topic, "before");

        var broken = this.producerFactory.Created.Single();
        broken.ErrorHandler(new Error(ErrorCode.Local_Fatal, "fatal error", isFatal: true));

        await bus.EmitAsync(Topic, "after");

        // librdkafka помечает продюсер с фатальной ошибкой окончательно сломанным,
        // поэтому отправка должна уйти через пересобранный экземпляр.
        this.producerFactory.Created.Should().HaveCount(2);
        this.producerFactory.Created.Last().Produced.Should().ContainSingle()
            .Which.Payload.Should().Be("after");
        broken.IsDisposed.Should().BeTrue();
        bus.IsHealthy.Should().BeTrue();

        await bus.Stop();
    }

    [Test]
    public async Task Emit_before_start_is_rejected()
    {
        var bus = await CreateBus();

        var emit = () => bus.EmitAsync(Topic, "payload");

        await emit.Should().ThrowAsync<BusNotReadyException>();
    }
}
