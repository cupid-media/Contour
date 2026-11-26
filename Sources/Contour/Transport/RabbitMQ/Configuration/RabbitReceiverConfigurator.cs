using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Receiving.Consumers;

namespace Contour.Transport.RabbitMQ.Configuration;

public class RabbitReceiverConfigurator
{
    internal RabbitReceiverConfigurator(MessageLabel label, RabbitReceiverOptions options)
    {
        Options = options with
        {
            Label = label
        };
        Label = label;
    }

    public MessageLabel Label { get; }

    /// <summary>
    /// Настройки получателя.
    /// </summary>
    internal RabbitReceiverOptions Options { get; }

    /// <summary>
    /// Регистрирует стратегию обработки сообщений, получение которых завершилось провалом.
    /// </summary>
    /// <param name="failedDeliveryStrategy">Стратегия обработки сообщений, получение которых завершилось провалом.</param>
    /// <returns>Конфигуратор получателя с установленной стратегией обработки сообщений.</returns>
    public RabbitReceiverConfigurator OnFailed(IFailedDeliveryStrategy failedDeliveryStrategy)
    {
        Options.FailedDeliveryStrategy = failedDeliveryStrategy;
        return this;
    }

    /// <summary>Регистрирует обработчик сообщений, получение которых завершилось провалом.</summary>
    /// <param name="failedDeliveryHandler">Обработчик сообщений, получение которых завершилось провалом.</param>
    /// <returns>Конфигуратор получателя с установленной стратегией обработки сообщений</returns>
    public RabbitReceiverConfigurator OnFailed(Func<IFailedConsumingContext, Task> failedDeliveryHandler)
    {
        return OnFailed(new LambdaFailedDeliveryStrategy(failedDeliveryHandler));
    }

    /// <summary>Регистрирует фабрику обработчиков входящего сообщения.</summary>
    /// <param name="consumerFactoryFunc">Фабрика обработчиков входящих сообщений.</param>
    /// <typeparam name="TModel">Тип обрабатываемого сообщения.</typeparam>
    /// <returns>Конфигуратор получателя с установленным обработчиком входящих сообщений.</returns>
    public RabbitReceiverConfigurator ReactWith<TModel>(Func<IConsumerOf<TModel>> consumerFactoryFunc) where TModel: class
    {
        Options.ReceiverRegistration = l => l.RegisterConsumer(new FactoryConsumerOf<TModel>(consumerFactoryFunc));
        return this;
    }

    /// <summary>Регистрирует обработчик входящего сообщения.</summary>
    /// <param name="consumer">Обработчик входящего сообщения.</param>
    /// <returns>Конфигуратор получателя с установленным обработчиком входящих сообщений.</returns>
    public RabbitReceiverConfigurator ReactWith<TModel>(IConsumer<TModel> consumer) where TModel: class
    {
        Options.ReceiverRegistration = l => l.RegisterConsumer(consumer);
        return this;
    }

    /// <summary>
    /// Устанавливает необходимость явного подтверждения успешной обработки сообщения.
    /// </summary>
    /// <returns>
    /// Конфигуратор получателя с явным подтверждением успешной обработки сообщения.
    /// </returns>
    public RabbitReceiverConfigurator RequiresAccept(bool requiresAccept = true)
    {
        Options.AcceptRequired = requiresAccept;
        return this;
    }

    public RabbitReceiverConfigurator ReuseConnection(bool reuse = true)
    {
        Options.ReuseConnection = reuse;
        return this;
    }

    public void Validate()
    {
        if (Options.Label.IsEmpty)
        {
            throw new InvalidOperationException("Can't receive using Empty label.");
        }
    }

    public RabbitReceiverConfigurator WithQoS(QoSParams qosParams)
    {
        Options.QoS = qosParams;
        return this;
    }

    public RabbitReceiverConfigurator WithConnectionString(string connectionString)
    {
        Options.ConnectionString = connectionString;

        return this;
    }

    /// <summary>
    /// Регистрирует построитель порта, по которому проходит получение входящих сообщений.
    /// </summary>
    /// <param name="endpointBuilder">Построитель порта входящих сообщений.</param>
    /// <returns>Конфигуратор получателя с установленным построителем порта входящих сообщений.</returns>
    public RabbitReceiverConfigurator WithEndpoint(Func<RabbitSubscriptionEndpointBuilderParams, CancellationToken, Task<ISubscriptionEndpoint>> endpointBuilder)
    {
        Options.EndpointBuilder = endpointBuilder;
        return this;
    }

    /// <summary>
    /// Устанавливает количество одновременных обработчиков входящих сообщений.
    /// </summary>
    /// <param name="parallelismLevel">Количество одновременных обработчиков входящих сообщений.</param>
    /// <returns>Конфигуратор получателя с установленным количеством одновременных обработчиков входящих сообщений.</returns>
    public RabbitReceiverConfigurator WithParallelismLevel(uint parallelismLevel)
    {
        if (parallelismLevel <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(parallelismLevel), "Parallelism level must be greater than zero");
        }

        Options.ParallelismLevel = parallelismLevel;
        return this;
    }

    /// <summary>
    /// Устанавливает хранилище заголовков входящего сообщения.
    /// </summary>
    /// <param name="storage">Хранилище заголовков входящего сообщения.</param>
    /// <returns>Конфигуратор получателя с установленным хранилищем заголовков.</returns>
    public RabbitReceiverConfigurator WithIncomingMessageHeaderStorage(IIncomingMessageHeaderStorage storage)
    {
        Options.IncomingMessageHeaderStorage = storage;
        return this;
    }

    public RabbitReceiverConfigurator WithQueueLimit(int queueLimit)
    {
        if (queueLimit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(queueLimit), "Queue limit must be positive");
        }

        Options.QueueLimit = queueLimit;
        return this;
    }

    public RabbitReceiverConfigurator WithQueueMaxLengthBytes(int bytes)
    {
        if (bytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes), "Queue max length bytes must be positive");
        }

        Options.QueueMaxLengthBytes = bytes;
        return this;
    }

    public RabbitReceiverConfigurator Delayed(bool delayed = true)
    {
        Options.Delayed = delayed;
        return this;
    }
}
