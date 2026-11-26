using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Sending;
using Contour.Tests.Mocks.Kafka;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.Kafka.Internal;
using FluentAssertions;
using NUnit.Framework;
using KafkaHeaders = Confluent.Kafka.Headers;

namespace Contour.Tests.KafkaBusSpecs;

[TestFixture]
[Category("Unit")]
public class KafkaRequestReplyTests
{
    private const string EndpointName = "rr-endpoint";

    private const string RequestTopic = "request-topic";

    /// <summary>
    ///   Топик ответов задать на уровне шины нельзя: он всегда производится от имени
    ///   конечной точки и метки маршрута — либо задаётся маршруту через
    ///   <see cref="KafkaSenderConfigurator.WithCallbackTopic"/>.
    /// </summary>
    private const string ReplyTopic = $"{EndpointName}.{RequestTopic}..reply";

    private FakeKafkaBroker broker;

    [SetUp]
    public void SetUp()
    {
        this.broker = new FakeKafkaBroker();
    }

    private Task<IBus> CreateBus(Action<KafkaBusConfigurator> configure)
    {
        return BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ConsumerFactory = this.broker.ConsumerFactory;
            cfg.ProducerFactory = this.broker.ProducerFactory;
            cfg.AutoStart = false;

            cfg.WithEndpointName(EndpointName)
                .WithConnectionString("fake:9092")
                .WithGroupId("rr-group")
                .WithPayloadConverter(new PassThroughPayloadConverter());

            configure(cfg);
        });
    }

    /// <summary>Обработчик запросов, отвечающий преобразованием строки.</summary>
    private sealed class UppercaseResponder : IConsumerOf<string>
    {
        public Func<string, Task<string>> Callback { get; set; }

        public async Task HandleAsync(IConsumingContext<string> context)
        {
            var request = context.Message.Payload;

            var response = Callback != null
                ? await Callback(request)
                : request.ToUpperInvariant();

            if (response != null)
            {
                await context.ReplyAsync(response);
            }
        }
    }

    [Test]
    public async Task Request_receives_the_response_from_the_responder()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        var result = await bus.RequestAsync<string, string>(RequestTopic, "ping")
            .WaitAsync(TimeSpan.FromSeconds(10));

        result.Response.Should().Be("PING");

        await bus.Stop();
    }

    /// <summary>Ответчик, пытающийся ограничить время жизни ответа.</summary>
    private sealed class ExpiringResponder : IConsumerOf<string>
    {
        public TaskCompletionSource<Exception> Outcome { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task HandleAsync(IConsumingContext<string> context)
        {
            try
            {
                await context.ReplyAsync("pong", Expires.In(TimeSpan.FromSeconds(1)));
                this.Outcome.TrySetResult(null);
            }
            catch (Exception ex)
            {
                this.Outcome.TrySetResult(ex);
            }
        }
    }

    [Test]
    public async Task Replying_with_an_expiration_is_rejected()
    {
        var responder = new ExpiringResponder();

        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(responder);
        });

        await bus.Start();

        // Запрос останется без ответа, поэтому ждём исход самого обработчика, а не RequestAsync.
        _ = bus.RequestAsync<string, string>(RequestTopic, "ping");

        var captured = await responder.Outcome.Task.WaitAsync(TimeSpan.FromSeconds(10));

        captured.Should().BeOfType<BusConfigurationException>(
            "у сообщения Kafka нет своего времени жизни — его определяет политика хранения топика, " +
            "и принять заголовок, ничего не сделав, значило бы дать ложную гарантию");

        this.broker.Published(ReplyTopic).Should().BeEmpty();

        await bus.Stop();
    }

    [Test]
    public async Task Request_carries_the_reply_route_and_correlation_id()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        await bus.RequestAsync<string, string>(RequestTopic, "ping").WaitAsync(TimeSpan.FromSeconds(10));

        var request = this.broker.Published(RequestTopic).Should().ContainSingle().Subject;
        var requestHeaders = Contour.Transport.Kafka.Internal.KafkaHeaderConverter.FromKafka(request.Headers);

        requestHeaders[Headers.ReplyRoute].Should().Be(ReplyTopic);
        requestHeaders.Should().ContainKey(Headers.CorrelationId);
        requestHeaders.Should().NotContainKey(Headers.Timeout, "the timeout is a local concern and must not be sent");

        var reply = this.broker.Published(ReplyTopic).Should().ContainSingle().Subject;
        var replyHeaders = Contour.Transport.Kafka.Internal.KafkaHeaderConverter.FromKafka(reply.Headers);

        replyHeaders[Headers.CorrelationId].Should().Be(
            requestHeaders[Headers.CorrelationId],
            "the reply must be matched to its request by correlation id");

        await bus.Stop();
    }

    [Test]
    public async Task Reply_continues_the_trace_of_the_request()
    {
        const string TraceId = "12345678901234567890123456789012";
        const string RequestSpanId = "1234567890123456";

        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        var options = new RequestOptions
        {
            AdditionalHeaders = new Dictionary<string, object>
            {
                [Headers.TraceParent] = $"00-{TraceId}-{RequestSpanId}-01",
                [Headers.TraceState] = "rojo=00f067aa0ba902b7"
            }
        };

        await bus.RequestAsync<string, string>(RequestTopic, "ping", options).WaitAsync(TimeSpan.FromSeconds(10));

        var reply = this.broker.Published(ReplyTopic).Should().ContainSingle().Subject;
        var replyHeaders = Contour.Transport.Kafka.Internal.KafkaHeaderConverter.FromKafka(reply.Headers);

        var traceParent = Headers.GetString(replyHeaders, Headers.TraceParent).Split('-');
        traceParent.Should().HaveCount(4);
        traceParent[1].Should().Be(TraceId, "the reply belongs to the trace of its request");
        traceParent[2].Should().NotBe(RequestSpanId, "the reply is a span of its own");
        Headers.GetString(replyHeaders, Headers.TraceState).Should().Be("rojo=00f067aa0ba902b7");

        await bus.Stop();
    }

    [Test]
    public async Task Concurrent_requests_get_their_own_responses()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder
            {
                // Первый запрос отвечает медленнее второго: ответы приходят не в порядке отправки.
                Callback = async payload =>
                {
                    await Task.Delay(payload == "slow" ? 300 : 10);
                    return payload.ToUpperInvariant();
                }
            });
        });

        await bus.Start();

        var slow = bus.RequestAsync<string, string>(RequestTopic, "slow");
        var fast = bus.RequestAsync<string, string>(RequestTopic, "fast");

        var results = await Task.WhenAll(slow, fast).WaitAsync(TimeSpan.FromSeconds(10));

        results[0].Response.Should().Be("SLOW");
        results[1].Response.Should().Be("FAST");

        await bus.Stop();
    }

    [Test]
    public async Task Request_times_out_when_no_response_arrives()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();

            // Получателя нет: запрос уходит, отвечать некому.
        });

        await bus.Start();

        var request = () => bus.RequestAsync<string, string>(RequestTopic, "ping", new RequestOptions
        {
            Timeout = TimeSpan.FromMilliseconds(300)
        });

        await request.Should().ThrowAsync<TimeoutException>();

        await bus.Stop();
    }

    [Test]
    public async Task Request_on_a_route_without_callback_is_not_supported()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic);
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        var request = () => bus.RequestAsync<string, string>(RequestTopic, "ping");

        (await request.Should().ThrowAsync<NotSupportedException>())
            .WithMessage("*WithDefaultCallbackEndpoint*");

        await bus.Stop();
    }

    [Test]
    public async Task Route_level_request_timeout_is_applied()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic)
                .WithDefaultCallbackEndpoint()
                .WithRequestTimeout(TimeSpan.FromMilliseconds(300));
        });

        await bus.Start();

        var request = () => bus.RequestAsync<string, string>(RequestTopic, "ping");

        await request.Should().ThrowAsync<TimeoutException>();

        await bus.Stop();
    }

    [Test]
    public async Task Pending_requests_are_cancelled_when_the_bus_stops()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
        });

        await bus.Start();

        var request = bus.RequestAsync<string, string>(RequestTopic, "ping");

        await bus.Stop();

        var awaiting = () => request;

        await awaiting.Should().ThrowAsync<OperationCanceledException>();
    }

    [Test]
    public async Task Responder_replies_only_when_a_reply_route_is_present()
    {
        var responder = new UppercaseResponder();

        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic);
            cfg.On(RequestTopic).WithReplies().ReactWith(responder);
        });

        await bus.Start();

        // Обычный EmitAsync: заголовка x-reply-route нет, отвечать некуда.
        await bus.EmitAsync(RequestTopic, "ping");

        await Wait.Until(() => this.broker.Published(RequestTopic).Count == 1);
        await Task.Delay(300);

        this.broker.Published(ReplyTopic).Should().BeEmpty("a one-way message must not produce a reply");

        await bus.Stop();
    }

    [Test]
    public async Task Reply_topic_is_derived_from_the_endpoint_name_and_the_route_label()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        await bus.RequestAsync<string, string>(RequestTopic, "ping").WaitAsync(TimeSpan.FromSeconds(10));

        this.broker.Published($"{EndpointName}.{RequestTopic}..reply").Should().ContainSingle();

        await bus.Stop();
    }

    [Test]
    public async Task Each_request_reply_route_gets_its_own_reply_topic()
    {
        // Топик ответов производится от метки, поэтому ответы двух маршрутов не смешиваются
        // и каждому не приходится вычитывать чужие.
        const string OtherRequestTopic = "other-request-topic";

        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
            cfg.Route(OtherRequestTopic).WithDefaultCallbackEndpoint();
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
            cfg.On(OtherRequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        await bus.RequestAsync<string, string>(RequestTopic, "ping").WaitAsync(TimeSpan.FromSeconds(10));
        await bus.RequestAsync<string, string>(OtherRequestTopic, "pong").WaitAsync(TimeSpan.FromSeconds(10));

        this.broker.Published($"{EndpointName}.{RequestTopic}..reply").Should().ContainSingle();
        this.broker.Published($"{EndpointName}.{OtherRequestTopic}..reply").Should().ContainSingle();

        await bus.Stop();
    }

    [Test]
    public async Task Route_level_callback_topic_overrides_the_derived_one()
    {
        const string DedicatedReplyTopic = "dedicated-reply-topic";

        var bus = await CreateBus(cfg =>
        {
            cfg.Route(RequestTopic).WithCallbackTopic(DedicatedReplyTopic);
            cfg.On(RequestTopic).WithReplies().ReactWith(new UppercaseResponder());
        });

        await bus.Start();

        await bus.RequestAsync<string, string>(RequestTopic, "ping").WaitAsync(TimeSpan.FromSeconds(10));

        this.broker.Published(DedicatedReplyTopic).Should().ContainSingle();
        this.broker.Published(ReplyTopic).Should().BeEmpty("the dedicated topic replaces the derived one");

        await bus.Stop();
    }

    [Test]
    public async Task Response_awaited_by_another_instance_is_skipped_without_the_unhandled_strategy()
    {
        var unhandled = new ConcurrentQueue<string>();

        var bus = await CreateBus(cfg =>
        {
            cfg.OnUnhandled(context =>
            {
                unhandled.Enqueue(((KafkaDelivery)context.Delivery).CorrelationId ?? string.Empty);
                return Task.CompletedTask;
            });
            cfg.Route(RequestTopic).WithDefaultCallbackEndpoint();
        });

        await bus.Start();

        this.broker.Publish(ReplyTopic, Encoding.UTF8.GetBytes("foreign"), WithCorrelationId("other-instance"));
        this.broker.Publish(ReplyTopic, Encoding.UTF8.GetBytes("not a response"));

        await Wait.Until(() => !unhandled.IsEmpty);

        unhandled.Should().ContainSingle().Which.Should().BeEmpty(
            "only the message without a correlation id is unhandled, the foreign response is skipped");

        await bus.Stop();
    }

    private static KafkaHeaders WithCorrelationId(string correlationId)
    {
        return new KafkaHeaders { { Headers.CorrelationId, Encoding.UTF8.GetBytes(correlationId) } };
    }
}
