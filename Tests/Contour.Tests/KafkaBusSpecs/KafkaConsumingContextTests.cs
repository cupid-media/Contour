using System;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Receiving.Consumers;
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
public class KafkaConsumingContextTests
{
    private const string Topic = "ctx-source";

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

            cfg.WithEndpointName("ctx-endpoint")
                .WithConnectionString("fake:9092")
                .WithGroupId("ctx-group")
                .WithPayloadConverter(new JsonPayloadConverter());

            configure(cfg);
        });
    }

    private void Publish(string payload, string topic = Topic)
    {
        this.broker.Publish(topic, JsonSerializer.SerializeToUtf8Bytes(payload));
    }

    private FakeKafkaConsumer SourceConsumer => this.broker.Consumers(Topic).Should().ContainSingle().Subject;

    /// <summary>Обработчик, получающий контекст обработки.</summary>
    private sealed class ContextConsumer : IConsumerOf<string>
    {
        public ConcurrentQueue<string> Handled { get; } = new();

        public Func<IConsumingContext<string>, Task> Callback { get; set; }

        public async Task HandleAsync(IConsumingContext<string> context)
        {
            Handled.Enqueue(context.Message.Payload);

            if (Callback != null)
            {
                await Callback(context);
            }
        }
    }

    [Test]
    public async Task Context_consumer_receives_the_message()
    {
        var consumer = new ContextConsumer();

        var bus = await CreateBus(cfg => cfg.On(Topic).ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        consumer.Handled.Should().ContainSingle().Which.Should().Be("payload");

        await bus.Stop();
    }

    [Test]
    public async Task Forward_moves_the_message_to_another_topic()
    {
        var consumer = new ContextConsumer
        {
            Callback = context => context.ForwardAsync("ctx-target", context.Message.Payload)
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).WithReplies().ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => this.broker.Published("ctx-target").Count == 1);

        var forwarded = this.broker.Published("ctx-target")[0];
        var headers = KafkaHeaderConverter.FromKafka(forwarded.Headers);

        Encoding.UTF8.GetString(forwarded.Value).Should().Be("\"payload\"");
        headers[Headers.MessageLabel].Should().Be(Topic, "the forwarded message remembers where it came from");
        headers[Headers.Breadcrumbs].Should().Be("ctx-endpoint");

        await bus.Stop();
    }

    [Test]
    public async Task Reject_holds_the_commit_position()
    {
        var consumer = new ContextConsumer
        {
            Callback = context => context.RejectAsync(requeue: true)
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        await bus.Stop();

        SourceConsumer.Commits.Should().BeEmpty("a rejected message must stay unread");
    }

    [Test]
    public async Task Accept_advances_the_commit_position_when_it_is_required()
    {
        var consumer = new ContextConsumer
        {
            Callback = context => context.AcceptAsync()
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).RequiresAccept().ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        await bus.Stop();

        SourceConsumer.Commits.Should().NotBeEmpty("the message was accepted");

        await Task.CompletedTask;
    }

    [Test]
    public async Task Without_accept_the_position_is_held_when_accept_is_required()
    {
        var consumer = new ContextConsumer();

        var bus = await CreateBus(cfg => cfg.On(Topic).RequiresAccept().ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        await bus.Stop();

        SourceConsumer.Commits.Should().BeEmpty("RequiresAccept demands an explicit Accept");
    }

    [Test]
    public async Task Accept_made_after_the_handler_returned_advances_the_commit_position()
    {
        // Так устроены накопительные обработчики: сообщение кладётся в пачку, управление
        // возвращается сразу, а подтверждение приходит позже.
        var deferred = new TaskCompletionSource<IConsumingContext<string>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumer = new ContextConsumer
        {
            Callback = context =>
            {
                deferred.TrySetResult(context);
                return Task.CompletedTask;
            }
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).RequiresAccept().ReactWith(consumer));
        await bus.Start();

        Publish("payload");

        var context = await deferred.Task.WaitAsync(TimeSpan.FromSeconds(5));
        SourceConsumer.Commits.Should().BeEmpty("the handler has not acknowledged the message yet");

        await context.AcceptAsync();

        await bus.Stop();

        SourceConsumer.Commits.Should().NotBeEmpty("the deferred accept must be honoured");
    }

    [Test]
    public async Task A_deferred_acknowledgement_does_not_block_the_next_message()
    {
        var pending = new ConcurrentQueue<IConsumingContext<string>>();

        var consumer = new ContextConsumer
        {
            Callback = context =>
            {
                pending.Enqueue(context);
                return Task.CompletedTask;
            }
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).RequiresAccept().ReactWith(consumer));
        await bus.Start();

        Publish("first");
        Publish("second");

        // Ни одно сообщение ещё не подтверждено, но воркер прочитал оба.
        await Wait.Until(() => consumer.Handled.Count == 2);

        while (pending.TryDequeue(out var context))
        {
            await context.AcceptAsync();
        }

        await bus.Stop();

        SourceConsumer.Commits.Should().NotBeEmpty();
    }

    [Test]
    public async Task A_deferred_acknowledgement_that_never_arrives_holds_the_position()
    {
        var consumer = new ContextConsumer();

        var bus = await CreateBus(cfg => cfg.On(Topic).RequiresAccept().ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        // Остановка снимает ожидание подтверждения: сообщение перечитается после перезапуска.
        // Таймаут нужен, чтобы зависшая остановка роняла тест, а не весь прогон.
        await bus.Stop().WaitAsync(TimeSpan.FromSeconds(10));

        SourceConsumer.Commits.Should().BeEmpty();
    }

    [Test]
    public async Task Stop_completes_while_some_deferred_acknowledgements_never_arrive()
    {
        var pending = new ConcurrentQueue<IConsumingContext<string>>();

        var consumer = new ContextConsumer
        {
            Callback = context =>
            {
                pending.Enqueue(context);
                return Task.CompletedTask;
            }
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).RequiresAccept().ReactWith(consumer));
        await bus.Start();

        Publish("first");
        Publish("second");

        await Wait.Until(() => consumer.Handled.Count == 2);

        // Подтверждается только первое: второе так и висит до остановки.
        pending.TryDequeue(out var first).Should().BeTrue();
        await first.AcceptAsync();

        await bus.Stop().WaitAsync(TimeSpan.FromSeconds(10));

        // Коммит доходит до первого, но не проходит за неподтверждённое второе.
        SourceConsumer.Commits.Should().NotBeEmpty()
            .And.OnlyContain(c => (long)c.Offset == 1L);
    }

    [Test]
    public async Task Reject_without_requeue_skips_the_message()
    {
        var consumer = new ContextConsumer
        {
            Callback = context => context.RejectAsync(requeue: false)
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        await bus.Stop();

        SourceConsumer.Commits.Should().NotBeEmpty(
            "в Kafka нельзя вернуть одно сообщение в очередь, поэтому отказ без перечитывания его пропускает");
    }

    [Test]
    public async Task A_plain_consumer_is_accepted_automatically()
    {
        var consumer = new TestKafkaConsumerOf<string>();

        var bus = await CreateBus(cfg => cfg.On(Topic).ReactWith(consumer));
        await bus.Start();

        Publish("payload");
        await Wait.Until(() => consumer.Handled.Count == 1);

        await bus.Stop();

        SourceConsumer.Commits.Should().NotBeEmpty();
    }

    [Test]
    public async Task Replying_to_a_one_way_message_does_nothing()
    {
        // Ждём именно завершения обработчика: Handled пополняется раньше, чем он доходит до конца.
        var finished = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        var consumer = new ContextConsumer
        {
            Callback = async context =>
            {
                context.CanReply.Should().BeFalse("сообщение пришло не как запрос");

                try
                {
                    await context.ReplyAsync("response");
                    finished.TrySetResult(null);
                }
                catch (Exception ex)
                {
                    finished.TrySetResult(ex);
                }
            }
        };

        var bus = await CreateBus(cfg => cfg.On(Topic).WithReplies().ReactWith(consumer));
        await bus.Start();

        Publish("payload");

        var captured = await finished.Task.WaitAsync(TimeSpan.FromSeconds(10));

        captured.Should().BeNull(
            "контекст обработки общий с RabbitMQ, а там ответ на сообщение без обратного адреса " +
            "молча не отправляется — расходиться в поведении между транспортами хуже");

        await bus.Stop();

        SourceConsumer.Commits.Should().NotBeEmpty("обработчик завершился без ошибки");
    }
}
