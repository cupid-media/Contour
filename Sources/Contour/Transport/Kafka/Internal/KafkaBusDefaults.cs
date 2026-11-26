using System;
using System.Collections.Generic;
using System.Reflection;
using Contour.Sending;
using Contour.Transport.Kafka.Configuration;

namespace Contour.Transport.Kafka.Internal;

internal static class KafkaBusDefaults
{
    private static MessageHeaderStorage defaultMessageHeaderStorage;

    public static readonly Endpoint DefaultEndpoint = new(Assembly.GetEntryAssembly()?.GetName().Name);

    public static KafkaReceiverOptions DefaultReceiverOptions => new()
    {
        ParallelismLevel = 1,
        EnableAutoCommit = false,
        IncomingMessageHeaderStorage = GetDefaultMessageHeaderStorage(),
        CommitOffset = 1000,
        CommitTimeout = TimeSpan.FromSeconds(5),
        MaxPendingAcknowledgements = 10_000
    };

    public static KafkaSenderOptions DefaultSenderOptions => new()
    {
        ConfirmationRequired = true,
        FailoverAttempts = 3,
        FlushTimeout = TimeSpan.FromSeconds(10),
        ProducerSelectorBuilder = new DefaultProducerSelectorBuilder(),
        IncomingMessageHeaderStorage = GetDefaultMessageHeaderStorage()
    };

    /// <summary>
    ///   Настройки получателя ответов: своя группа на экземпляр шины и чтение только новых
    ///   сообщений — старые ответы уже никого не ждут. В Kafka нет адресуемой очереди для одного
    ///   клиента, поэтому ответы получают все экземпляры, а каждый оставляет только те, чей
    ///   <c>x-correlation-id</c> он ждёт.
    /// </summary>
    public static KafkaReceiverOptions CallbackReceiverOptions(KafkaReceiverOptions defaults, string replyTopic, string endpointName)
    {
        var options = defaults.CloneFor(replyTopic);

        options.GroupId = $"{endpointName}.reply.{Guid.NewGuid():N}";
        options.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Latest;
        options.EnableAutoCommit = false;
        options.ParallelismLevel = 1;

        // Ответы не подтверждаются: оффсеты этой одноразовой группы никому не нужны,
        // а коммит только мешал бы — группа живёт не дольше экземпляра шины.
        options.CommitOffset = int.MaxValue;
        options.CommitTimeout = TimeSpan.FromDays(1);

        // Ответы не обрабатываются подписчиками, и переложить их некуда. Стратегия
        // необработанных остаётся: в топик ответов может попасть сообщение, не похожее на ответ.
        options.ReceiverRegistration = null;
        options.FailedDeliveryStrategy = null;
        options.AcceptRequired = false;

        return options;
    }

    /// <summary>
    ///   Общее с RabbitMQ хранилище заголовков входящих сообщений: заголовки, относящиеся
    ///   к транспорту, не переносятся в исходящие сообщения.
    /// </summary>
    public static MessageHeaderStorage GetDefaultMessageHeaderStorage()
    {
        if (defaultMessageHeaderStorage != null)
        {
            return defaultMessageHeaderStorage;
        }

        var blockedHeaders = new List<string>
        {
            Headers.Expires,
            Headers.MessageLabel,
            Headers.Persist,
            Headers.QueueMessageTtl,
            Headers.ReplyRoute,
            Headers.Timeout,
            Headers.Ttl,
            Headers.QueueMaxLength,
            Headers.QueueMaxLengthBytes,
            Headers.Delay
        };

        defaultMessageHeaderStorage = new MessageHeaderStorage(blockedHeaders);

        Headers.GlobalStorage ??= defaultMessageHeaderStorage;

        return defaultMessageHeaderStorage;
    }
}
