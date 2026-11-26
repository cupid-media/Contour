using System;
using System.Text.Json;
using Confluent.Kafka;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal sealed class ConfluentKafkaProducerFactory : IKafkaProducerFactory
{
    private static readonly ILogger Logger = LogManager.GetLogger<ConfluentKafkaProducerFactory>();

    public IProducer<string, byte[]> Create(KafkaSenderOptions options, Action<Error> errorHandler)
    {
        return new ProducerBuilder<string, byte[]>(options.BuildProducerConfig())
            .SetErrorHandler((producer, error) =>
            {
                Logger.LogError(
                    "Kafka producer error. Producer: {Producer}, Label: {Label}, error: {Error}",
                    producer.Name, options.Label, JsonSerializer.Serialize(error));

                errorHandler?.Invoke(error);
            })
            .SetLogHandler((producer, message) => LogBrokerMessage(options, message))
            .Build();
    }

    private static void LogBrokerMessage(KafkaSenderOptions options, LogMessage message)
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
            "Kafka broker log. Label: {Label}, {Level}: {Message}",
            options.Label, message.Level, JsonSerializer.Serialize(message));
    }
}
