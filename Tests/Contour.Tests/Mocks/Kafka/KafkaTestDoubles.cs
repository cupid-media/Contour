using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Serialization;
using Contour.Transport.Kafka;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.Kafka.Internal;

namespace Contour.Tests.Mocks.Kafka;

/// <summary>
///   Фабрика, раздающая фейковые консьюмеры и запоминающая созданные экземпляры.
/// </summary>
internal sealed class FakeKafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly ConcurrentQueue<FakeKafkaConsumer> prepared = new();

    public ConcurrentQueue<FakeKafkaConsumer> Created { get; } = new();

    public void Prepare(FakeKafkaConsumer consumer)
    {
        this.prepared.Enqueue(consumer);
    }

    public IConsumer<Ignore, byte[]> Create(
        KafkaReceiverOptions options,
        PartitionsRevokedHandler partitionsRevokedHandler)
    {
        if (!this.prepared.TryDequeue(out var consumer))
        {
            consumer = new FakeKafkaConsumer();
        }

        consumer.PartitionsRevokedHandler = partitionsRevokedHandler;
        Created.Enqueue(consumer);
        return consumer;
    }
}

/// <summary>
///   Фабрика, не умеющая создать консьюмера: имитирует отвергнутую librdkafka конфигурацию.
/// </summary>
internal sealed class ThrowingKafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly Exception exception;

    public ThrowingKafkaConsumerFactory(Exception exception)
    {
        this.exception = exception;
    }

    public IConsumer<Ignore, byte[]> Create(
        KafkaReceiverOptions options,
        PartitionsRevokedHandler partitionsRevokedHandler)
    {
        throw this.exception;
    }
}

/// <summary>
///   Конвертер полезной нагрузки «как есть»: сообщение читается как UTF-8 строка без десериализации.
/// </summary>
internal sealed class PassThroughPayloadConverter : IPayloadConverter
{
    public string ContentType => "text/plain";

    public byte[] FromObject(object payload)
    {
        return Encoding.UTF8.GetBytes((string)payload);
    }

    public T ToObject<T>(byte[] payload)
    {
        return (T)(object)Encoding.UTF8.GetString(payload);
    }
}

/// <summary>
///   Тестовый консьюмер сообщений с настраиваемым обработчиком.
/// </summary>
internal sealed class TestKafkaConsumerOf<T> : IConsumerOf<T>
    where T : class
{
    public ConcurrentQueue<Message<T>> Handled { get; } = new();

    public Func<IConsumingContext<T>, Task> Callback { get; set; }

    public Task HandleAsync(IConsumingContext<T> context)
    {
        Handled.Enqueue(context.Message);
        return Callback?.Invoke(context) ?? Task.CompletedTask;
    }
}

/// <summary>
///   Фейковый ресивер для проверки замыканий регистрации консьюмеров.
/// </summary>
internal sealed class FakeKafkaReceiver : IReceiver
{
    public FakeKafkaReceiver(string topic = "topic")
    {
        Label = topic;
    }

    public string Label { get; }

    public string RegisteredTopic { get; private set; }

    public object RegisteredConsumer { get; private set; }

    public Type RegisteredMessageType { get; private set; }

    public bool IsHealthy => true;

    public int ActiveRequestsCount => 0;

    public bool IsStarted { get; private set; }

    public bool IsDisposed { get; private set; }

    public void RegisterConsumer<T>(IConsumer<T> consumer)
        where T : class
    {
        RegisteredTopic = Label;
        RegisteredConsumer = consumer;
        RegisteredMessageType = typeof(T);
    }

    public Task Prepare(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task Start(CancellationToken cancellationToken)
    {
        IsStarted = true;
        return Task.CompletedTask;
    }

    public Task Stop(CancellationToken cancellationToken)
    {
        IsStarted = false;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        IsDisposed = true;
        return ValueTask.CompletedTask;
    }
}

/// <summary>
///   Фейковый компонент шины для тестов трекера.
/// </summary>
internal sealed class FakeKafkaBusComponent : IBusComponent
{
    public bool IsStarted { get; private set; }

    public bool IsDisposed { get; private set; }

    public bool IsHealthy { get; set; } = true;

    public Task Prepare(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task Start(CancellationToken cancellationToken)
    {
        IsStarted = true;
        return Task.CompletedTask;
    }

    public Task Stop(CancellationToken cancellationToken)
    {
        IsStarted = false;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        IsDisposed = true;
        return ValueTask.CompletedTask;
    }
}

/// <summary>
///   Фабрика, раздающая фейковые продюсеры и запоминающая созданные экземпляры.
/// </summary>
internal sealed class FakeKafkaProducerFactory : IKafkaProducerFactory
{
    public ConcurrentQueue<FakeKafkaProducer> Created { get; } = new();

    public Confluent.Kafka.IProducer<string, byte[]> Create(KafkaSenderOptions options, Action<Error> errorHandler)
    {
        var producer = new FakeKafkaProducer { ErrorHandler = errorHandler };
        Created.Enqueue(producer);
        return producer;
    }
}

/// <summary>
///   Отправленное фейковым продюсером сообщение.
/// </summary>
internal sealed record ProducedMessage(string Topic, string Key, byte[] Value, IDictionary<string, object> Headers)
{
    public string Payload => Encoding.UTF8.GetString(Value);
}

internal static class Wait
{
    public static async Task Until(Func<bool> condition, int timeoutMs = 5000, string because = null)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Condition was not met in {timeoutMs} ms{(because == null ? string.Empty : ": " + because)}");
            }

            await Task.Delay(20);
        }
    }
}
