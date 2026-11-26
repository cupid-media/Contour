using System;
using System.Collections.Generic;
using System.Text.Json;
using Confluent.Kafka;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal sealed class ConfluentKafkaConsumerFactory : IKafkaConsumerFactory
{
    private static readonly ILogger Logger = LogManager.GetLogger<ConfluentKafkaConsumerFactory>();

    public IConsumer<Ignore, byte[]> Create(
        KafkaReceiverOptions options,
        PartitionsRevokedHandler partitionsRevokedHandler)
    {
        return new ConsumerBuilder<Ignore, byte[]>(options.BuildConsumerConfig())
            .SetLogHandler((consumer, message) => LogBrokerMessage(options, message))
            .SetErrorHandler((consumer, error) => Logger.LogError(
                "Kafka broker error. GroupId: {GroupId}, Label: {Label}, error: {Error}",
                options.GroupId, options.Label, JsonSerializer.Serialize(error)))
            .SetPartitionsAssignedHandler((consumer, partitions) =>
            {
                if (partitions.Count == 0)
                {
                    // Партиций в топике меньше, чем участников группы: этому экземпляру читать нечего,
                    // и он будет простаивать, пока группа не изменится или админ не добавит партиций.
                    Logger.LogWarning(
                        "No partitions assigned, consumer is idle: the group has more members than the topic has partitions. GroupId: {GroupId}, Label: {Label}",
                        options.GroupId, options.Label);
                    return;
                }

                Logger.LogInformation(
                    "Partitions assigned: {Partitions}. GroupId: {GroupId}, Label: {Label}",
                    string.Join(", ", partitions), options.GroupId, options.Label);
            })
            .SetPartitionsRevokedHandler((consumer, partitions) =>
            {
                Logger.LogInformation(
                    "Partitions revoked: {Partitions}. GroupId: {GroupId}, Label: {Label}",
                    string.Join(", ", partitions), options.GroupId, options.Label);

                partitionsRevokedHandler?.Invoke(consumer, partitions);
            })
            .Build();
    }

    private static void LogBrokerMessage(KafkaReceiverOptions options, LogMessage message)
    {
        var level = message.Level switch
        {
            SyslogLevel.Emergency => LogLevel.Error,
            SyslogLevel.Alert => LogLevel.Error,
            SyslogLevel.Critical => LogLevel.Error,
            SyslogLevel.Error => LogLevel.Error,
            SyslogLevel.Warning => LogLevel.Warning,
            SyslogLevel.Notice => LogLevel.Information,
            SyslogLevel.Info => LogLevel.Information,
            _ => LogLevel.Debug
        };

        Logger.Log(level,
            "Kafka broker log. GroupId: {GroupId}, Label: {Label}, {Level}: {Message}",
            options.GroupId, options.Label, message.Level, JsonSerializer.Serialize(message));
    }
}
