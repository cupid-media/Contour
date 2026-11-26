using System.Threading.Tasks;
using System;
using System.Threading;
using Contour.Transport.Kafka.Configuration;
using Contour.Transport.RabbitMQ.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour;

/// <summary>
///   Фабрика для создания клиента шины сообщений на основе конфигурации.
/// </summary>
public static class BusFactory
{
    public static void UseLogger(ILoggerFactory loggerFactory)
    {
        LogManager.Initialize(loggerFactory);
    }

    /// <summary>
    /// Создает экземпляр шины событий
    /// </summary>
    /// <param name="configure">
    /// Делегат, используемый для задания настроек шины событий
    /// </param>
    /// <param name="token"></param>
    /// <returns>
    /// Сконфигурированный экземпляр шины событий
    /// </returns>
    public static async Task<IBus> CreateRmqBus(Action<RabbitBusConfigurator> configure, CancellationToken token = default)
    {
        var configurator = new RabbitBusConfigurator();
        configure(configurator);
        configurator.PostConfigure();

        configurator.Validate();

        var bus = configurator.Create();

        if (configurator.AutoStart)
        {
            await bus.Start(token);
        }

        return bus;
    }

    /// <summary>
    /// Создает экземпляр шины событий поверх Kafka.
    /// </summary>
    /// <param name="configure">
    /// Делегат, используемый для задания настроек шины событий
    /// </param>
    /// <param name="token"></param>
    /// <returns>
    /// Сконфигурированный экземпляр шины событий
    /// </returns>
    public static async Task<IBus> CreateKafkaBus(Action<KafkaBusConfigurator> configure, CancellationToken token = default)
    {
        var configurator = new KafkaBusConfigurator();
        configure(configurator);
        configurator.PostConfigure();

        configurator.Validate();

        var bus = configurator.Create();

        if (configurator.AutoStart)
        {
            await bus.Start(token);
        }

        return bus;
    }
}
