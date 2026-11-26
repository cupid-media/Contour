using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
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
public class KafkaFaultTests
{
    private const string Topic = "fault-source";
    private const string FaultTopic = "fault-endpoint.fault";

    private FakeKafkaBroker broker;
    private TestKafkaConsumerOf<string> consumer;

    [SetUp]
    public void SetUp()
    {
        this.broker = new FakeKafkaBroker();
        this.consumer = new TestKafkaConsumerOf<string>();
    }

    private Task<IBus> CreateBus(Action<KafkaBusConfigurator> configure)
    {
        return BusFactory.CreateKafkaBus(cfg =>
        {
            cfg.ConsumerFactory = this.broker.ConsumerFactory;
            cfg.ProducerFactory = this.broker.ProducerFactory;
            cfg.AutoStart = false;

            cfg.WithEndpointName("fault-endpoint")
                .WithConnectionString("fake:9092")
                .WithGroupId("fault-group")
                .WithPayloadConverter(new JsonPayloadConverter());

            configure(cfg);
        });
    }

    private static KafkaFaultMessage ReadFault(FakeKafkaBroker broker, string topic)
    {
        var published = broker.Published(topic).Should().ContainSingle().Subject;

        return JsonSerializer.Deserialize<KafkaFaultMessage>(published.Value);
    }

    [Test]
    public async Task Failed_message_is_forwarded_to_the_fault_topic()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.broker.Published(FaultTopic).Count == 1);

        var fault = ReadFault(this.broker, FaultTopic);

        fault.Label.Should().Be(Topic);
        fault.Exception.Should().NotBeNull();
        fault.ContentType.Should().Be("application/json");

        await bus.Stop();
    }

    [Test]
    public async Task Malformed_payload_is_forwarded_to_the_fault_topic()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        // Падение произойдёт на десериализации, до вызова обработчика: это такой же провал
        // обработки, и разбирать его должна та же стратегия.
        this.broker.Publish(Topic, Encoding.UTF8.GetBytes("not-a-json"));

        await Wait.Until(() => this.broker.Published(FaultTopic).Count == 1);

        this.consumer.Handled.Should().BeEmpty("обработчик не должен был получить управление");
        ReadFault(this.broker, FaultTopic).Exception.Should().NotBeNull();

        await bus.Stop();
    }

    [Test]
    public async Task Malformed_payload_does_not_block_the_partition()
    {
        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        this.broker.Publish(Topic, Encoding.UTF8.GetBytes("not-a-json"));
        await ProduceRequest("payload");

        await Wait.Until(() => this.consumer.Handled.Count == 1);
        await bus.Stop();

        // Битое сообщение переложено, поэтому оффсет идёт дальше — и следующее за ним
        // сообщение тоже коммитится, а не удерживается вечно.
        var sourceConsumer = this.broker.Consumers(Topic).Should().ContainSingle().Subject;
        sourceConsumer.Commits.Select(c => (long)c.Offset).Should().Contain(2L);
    }

    [Test]
    public async Task Fault_message_keeps_the_original_payload_as_text()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.broker.Published(FaultTopic).Count == 1);

        // Нагрузка сохраняется как исходный текст, а не как разобранный объект:
        // обработчик мог упасть и на десериализации.
        ReadFault(this.broker, FaultTopic).Payload.ToString().Should().Be("\"payload\"");

        await bus.Stop();
    }

    [Test]
    public async Task Forwarded_message_advances_the_offset()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.broker.Published(FaultTopic).Count == 1);

        await bus.Stop();

        // Сообщение переложено, поэтому его оффсет продвигается: после рестарта
        // оно не будет прочитано снова.
        var sourceConsumer = this.broker.Consumers(Topic).Should().ContainSingle().Subject;
        sourceConsumer.Commits.Select(c => (long)c.Offset).Should().Contain(1L);
    }

    [Test]
    public async Task Failed_forward_commits_the_offset()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg =>
        {
            cfg.OnFailed(_ => throw new InvalidOperationException("fault topic is unavailable"));
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.consumer.Handled.Count >= 1);

        await bus.Stop();

        // Стратегия провалилась — оффсет всё равно сдвигаем.
        var sourceConsumer = this.broker.Consumers(Topic).Should().ContainSingle().Subject;
        sourceConsumer.Commits.Should().HaveCount(1, "сообщение считается обработанным, даже если стратегия провалилась");
    }

    [Test]
    public async Task Strategy_can_hold_the_offset_with_reject()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var bus = await CreateBus(cfg =>
        {
            cfg.OnFailed(async ctx =>
            {
                await ctx.Delivery.RejectAsync(requeue: true);
                handled.TrySetResult();
            });
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        await ProduceRequest("payload");
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await bus.Stop();

        // Стратегия сама решила удержать сообщение: оно перечитается после перезапуска.
        var sourceConsumer = this.broker.Consumers(Topic).Should().ContainSingle().Subject;
        sourceConsumer.Commits.Should().BeEmpty();
    }

    [Test]
    public async Task Without_a_strategy_the_failed_message_is_only_logged()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg => cfg.On(Topic).ReactWith(this.consumer));

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.consumer.Handled.Count >= 1);

        await bus.Stop();

        this.broker.Published(FaultTopic).Should().BeEmpty("no fault strategy is configured");

        await Task.CompletedTask;
    }

    [Test]
    public async Task Without_a_strategy_the_failed_message_advances_the_offset()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg => cfg.On(Topic).ReactWith(this.consumer));

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.consumer.Handled.Count >= 1);

        await bus.Stop();

        // Без стратегии упавшее сообщение считается обработанным: иначе его оффсет
        // навсегда остановил бы продвижение партиции.
        var sourceConsumer = this.broker.Consumers(Topic).Should().ContainSingle().Subject;
        sourceConsumer.Commits.Select(c => (long)c.Offset).Should().Contain(1L);
    }

    [Test]
    public async Task Fault_topic_follows_the_endpoint_name_set_after_the_strategy()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
            cfg.WithEndpointName("renamed-endpoint");
        });

        await bus.Start();

        await ProduceRequest("payload");
        await Wait.Until(() => this.broker.Published("renamed-endpoint.fault").Count == 1);

        this.broker.Published(FaultTopic).Should().BeEmpty();

        await bus.Stop();
    }

    [Test]
    public async Task Fault_message_carries_the_correlation_id_of_the_original()
    {
        this.consumer.Callback = _ => throw new InvalidOperationException("boom");

        var bus = await CreateBus(cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(Topic).ReactWith(this.consumer);
        });

        await bus.Start();

        var correlationId = Guid.NewGuid().ToString("n");
        await ProduceRequest("payload", Topic, correlationId);

        await Wait.Until(() => this.broker.Published(FaultTopic).Count == 1);

        var headers = KafkaHeaderConverter.FromKafka(this.broker.Published(FaultTopic).Single().Headers);

        headers[Headers.CorrelationId].Should().Be(correlationId);
        headers[Headers.MessageLabel].Should().Be(Topic);
        headers[Headers.Breadcrumbs].Should().Be("fault-endpoint");

        await bus.Stop();
    }

    private Task ProduceRequest(string payload, string topic = Topic, string correlationId = null)
    {
        var headers = new Confluent.Kafka.Headers();

        if (correlationId != null)
        {
            headers.Add(Headers.CorrelationId, Encoding.UTF8.GetBytes(correlationId));
        }

        this.broker.Publish(topic, JsonSerializer.SerializeToUtf8Bytes(payload), headers);

        return Task.CompletedTask;
    }
}
