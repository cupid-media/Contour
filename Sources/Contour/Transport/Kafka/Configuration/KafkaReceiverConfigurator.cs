using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Serialization;
using Contour.Transport.Kafka.Internal;

namespace Contour.Transport.Kafka.Configuration;

/// <summary>
///   Конфигуратор получателя сообщений одного топика. Каждый получатель работает
///   на собственном снимке настроек, поэтому дефолты шины, выставленные после
///   объявления топика, на него уже не влияют.
/// </summary>
public class KafkaReceiverConfigurator
{
    internal KafkaReceiverConfigurator(string topic, KafkaReceiverOptions options)
    {
        Options = options.CloneFor(topic);
        Topic = topic;
    }

    public string Topic { get; }

    /// <summary>
    ///   Настройки получателя.
    /// </summary>
    internal KafkaReceiverOptions Options { get; }

    /// <summary>Регистрирует обработчик сообщений топика.</summary>
    /// <param name="consumer">
    ///   Обработчик сообщений — тот же <see cref="IConsumerOf{T}"/>, что и у RabbitMQ.
    ///   Ответ на запрос отправляется из обработчика через <see cref="IConsumingContext{T}.ReplyAsync"/>.
    /// </param>
    /// <typeparam name="TModel">Тип полезной нагрузки сообщения.</typeparam>
    public KafkaReceiverConfigurator ReactWith<TModel>(IConsumerOf<TModel> consumer)
        where TModel : class
    {
        Options.ReceiverRegistration = r => r.RegisterConsumer(consumer);
        return this;
    }

    /// <summary>
    ///   Требует явного подтверждения обработки: без <see cref="IConsumingContext{T}.AcceptAsync"/>
    ///   оффсет сообщения не продвигается.
    /// </summary>
    public KafkaReceiverConfigurator RequiresAccept(bool requiresAccept = true)
    {
        Options.AcceptRequired = requiresAccept;
        return this;
    }

    /// <summary>
    ///   Разрешает обработчику отвечать на запросы и пересылать сообщения: для кластера, из
    ///   которого читает получатель, заводится продюсер.
    /// </summary>
    public KafkaReceiverConfigurator WithReplies(bool replies = true)
    {
        Options.Replies = replies;
        return this;
    }

    /// <summary>Регистрирует стратегию обработки провалов для этого топика.</summary>
    public KafkaReceiverConfigurator OnFailed(IFailedDeliveryStrategy failedDeliveryStrategy)
    {
        Options.FailedDeliveryStrategy = failedDeliveryStrategy;
        return this;
    }

    /// <summary>Регистрирует обработчик провалов для этого топика.</summary>
    public KafkaReceiverConfigurator OnFailed(Func<IFailedConsumingContext, Task> failedDeliveryHandler)
    {
        return OnFailed(new LambdaFailedDeliveryStrategy(failedDeliveryHandler));
    }
    public KafkaReceiverConfigurator WithBootstrapServers(string bootstrapServers)
    {
        Options.ConnectionString = bootstrapServers;
        return this;
    }

    public KafkaReceiverConfigurator WithGroupId(string groupId)
    {
        Options.GroupId = groupId;
        return this;
    }

    public KafkaReceiverConfigurator WithPayloadConverter(IPayloadConverter converter)
    {
        Options.PayloadConverter = converter;
        return this;
    }

    public KafkaReceiverConfigurator WithParallelismLevel(uint parallelismLevel)
    {
        if (parallelismLevel == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(parallelismLevel), "Parallelism level must be greater than zero");
        }

        Options.ParallelismLevel = parallelismLevel;
        return this;
    }

    /// <summary>
    ///   Коммит-чекпоинт: фиксировать позиции каждые <paramref name="commitOffset"/> обработанных сообщений.
    /// </summary>
    public KafkaReceiverConfigurator WithCommitOffset(int commitOffset)
    {
        if (commitOffset <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(commitOffset), "Commit offset must be positive");
        }

        Options.CommitOffset = commitOffset;
        return this;
    }

    /// <summary>
    ///   Максимальное время, которое обработанный хвост тихой партиции может оставаться незакоммиченным.
    /// </summary>
    public KafkaReceiverConfigurator WithCommitTimeout(TimeSpan commitTimeout)
    {
        if (commitTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(commitTimeout), "Commit timeout must be positive");
        }

        Options.CommitTimeout = commitTimeout;
        return this;
    }

    /// <summary>
    ///   Сколько прочитанных сообщений может одновременно ждать отложенного решения обработчика;
    ///   по достижении предела чтение приостанавливается.
    /// </summary>
    public KafkaReceiverConfigurator WithMaxPendingAcknowledgements(int maxPendingAcknowledgements)
    {
        if (maxPendingAcknowledgements <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxPendingAcknowledgements), "Max pending acknowledgements must be positive");
        }

        Options.MaxPendingAcknowledgements = maxPendingAcknowledgements;
        return this;
    }

    public KafkaReceiverConfigurator WithAutoOffsetReset(AutoOffsetReset autoOffsetReset)
    {
        Options.AutoOffsetReset = autoOffsetReset;
        return this;
    }

    public KafkaReceiverConfigurator WithAutoCommit(bool enableAutoCommit = true)
    {
        Options.EnableAutoCommit = enableAutoCommit;
        return this;
    }

    public KafkaReceiverConfigurator WithPartitionAssignmentStrategy(PartitionAssignmentStrategy strategy)
    {
        Options.PartitionAssignmentStrategy = strategy;
        return this;
    }

    public KafkaReceiverConfigurator WithSecurityProtocol(SecurityProtocol protocol)
    {
        Options.SecurityProtocol = protocol;
        return this;
    }

    public KafkaReceiverConfigurator WithSslCaPem(string sslCaPem)
    {
        Options.SslCaPem = sslCaPem;
        return this;
    }

    public KafkaReceiverConfigurator WithSslCertificatePem(string sslCertificatePem)
    {
        Options.SslCertificatePem = sslCertificatePem;
        return this;
    }

    public KafkaReceiverConfigurator WithSslKeyPem(string sslKeyPem)
    {
        Options.SslKeyPem = sslKeyPem;
        return this;
    }

    public KafkaReceiverConfigurator WithSslCertificateVerification(bool enableSslCertificateVerification = true)
    {
        Options.EnableSslCertificateVerification = enableSslCertificateVerification;
        return this;
    }

    public KafkaReceiverConfigurator WithIncomingMessageHeaderStorage(IIncomingMessageHeaderStorage storage)
    {
        Options.IncomingMessageHeaderStorage = storage;
        return this;
    }

    public KafkaReceiverConfigurator WithDebug(string debug)
    {
        Options.Debug = debug;
        return this;
    }

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Options.ConnectionString))
        {
            throw new BusConfigurationException(
                $"ConnectionString are not set for receiver of topic [{Topic}].");
        }

        if (string.IsNullOrWhiteSpace(Options.GroupId))
        {
            throw new BusConfigurationException(
                $"GroupId is not set for receiver of topic [{Topic}].");
        }

        if (Options.ReceiverRegistration == null)
        {
            throw new BusConfigurationException(
                $"No consumer is registered for topic [{Topic}]. Call ReactWith(consumer).");
        }

        if (Options.PayloadConverter == null)
        {
            throw new BusConfigurationException(
                $"PayloadConverter is not set for receiver of topic [{Topic}].");
        }
    }
}
