using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Sending;

namespace Contour.Transport.RabbitMQ.Configuration;

/// <summary>
/// Конфигурация отправителя сообщений.
/// Используется для конфигурирования вариантов односторонних коммуникаций и коммуникаций в формате запрос-ответ.
/// </summary>
public record RabbitSenderConfiguration
{
    internal RabbitSenderConfiguration(MessageLabel label, RabbitSenderOptions senderOptions)
    {
        if (!MessageLabel.IsValidLabel(label.Name))
        {
            throw new ArgumentException($"Invalid message label [{label.Name}].", nameof(label));
        }

        Label = label;
        Options = senderOptions with
        {
            Label = label,
            CallbackOptions = senderOptions.CallbackOptions with {}
        };
    }

    /// <summary>
    /// Метка отправляемых сообщений.
    /// </summary>
    public MessageLabel Label { get; }

    /// <summary>
    /// Настройки отправителя сообщений.
    /// </summary>
    public RabbitSenderOptions Options { get; }

    /// <summary>
    /// Дополняет конфигурацию.
    /// </summary>
    /// <param name="routeResolverBuilder">Построитель вычислителя маршрута.</param>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration ConfiguredWith(Func<RabbitRouteResolverBuilderParams, CancellationToken, ValueTask<IRouteResolver>> routeResolverBuilder)
    {
        Options.RouteResolverBuilder = routeResolverBuilder;
        return this;
    }

    /// <summary>
    /// Устанавливает необходимость сохранять сообщения на диске.
    /// </summary>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration Persistently(bool persistently = true)
    {
        Options.Persistently = persistently;
        return this;
    }

    public RabbitSenderConfiguration Delayed(bool delayed = true)
    {
        Options.Delayed = delayed;
        return this;
    }

    public RabbitSenderConfiguration WithConnectionString(string connectionString)
    {
        // Need to set the callback sender's connection string to use the same connection string both in sender and receiver in request-response scenario
        Options.ConnectionString = connectionString;
        Options.CallbackOptions.ConnectionString = connectionString;

        return this;
    }

    public RabbitSenderConfiguration ReuseConnection(bool? reuse = null)
    {
        Options.ReuseConnection = reuse ?? true;
        return this;
    }

    /// <summary>
    /// Проверяет корректность конфигурации.
    /// </summary>
    public void Validate()
    {
        if (Label.IsEmpty)
        {
            throw new BusConfigurationException("Can't send using Empty label.");
        }
    }

    /// <summary>
    /// Устанавливает псевдоним для метки сообщения.
    /// </summary>
    /// <param name="alias">Псевдоним метки сообщения.</param>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration WithAlias(string alias)
    {
        string tempAlias = MessageLabel.AliasPrefix + alias;
        if (!MessageLabel.IsValidAlias(tempAlias))
        {
            throw new ArgumentException($"Invalid message label alias [{alias}].", nameof(alias));
        }

        Options.Key = tempAlias;

        return this;
    }

    /// <summary>
    /// Устанавливает конечную точку обратного вызова для получения ответных сообщений.
    /// </summary>
    /// <param name="callbackEndpointBuilder">Построитель конечной точки для ответных сообщений.</param>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration WithCallbackEndpoint(Func<RabbitSubscriptionEndpointBuilderParams, CancellationToken, Task<ISubscriptionEndpoint>> callbackEndpointBuilder)
    {
        Options.RequiresCallback = true;
        Options.CallbackOptions.EndpointBuilder = callbackEndpointBuilder;
        return this;
    }

    /// <summary>
    /// Устанавливает необходимость подтверждения отправки сообщения.
    /// </summary>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration WithConfirmation(bool confirm = true, TimeSpan? timeout = null)
    {
        Options.ConfirmationRequired = confirm;
        Options.ConfirmationTimeout = confirm ? timeout : null;
        return this;
    }

    /// <summary>
    /// Устанавливает конечную точку обратного вызова по умолчанию для получения сообщений.
    /// </summary>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration WithDefaultCallbackEndpoint()
    {
        return WithCallbackEndpoint((seb, token) => seb.UseDefaultTempReplyEndpoint(Options, token));
    }

    /// <summary>
    /// Устанавливает временной интервал за который должен быть получен ответ на запрос.
    /// </summary>
    /// <param name="timeout">Временной интервал за который должен быть получен ответ на запрос.</param>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration WithRequestTimeout(TimeSpan? timeout)
    {
        Options.RequestTimeout = timeout;
        return this;
    }

    /// <summary>
    /// Устанавливает время жизни отправляемого сообщения.
    /// </summary>
    /// <param name="ttl">Время жизни отправляемого сообщения.</param>
    /// <returns>Конфигурация отправителя.</returns>
    public RabbitSenderConfiguration WithTtl(TimeSpan? ttl)
    {
        Options.Ttl = ttl;
        return this;
    }

    /// <summary>
    /// Устанавливает хранилище заголовков входящего сообщения.
    /// </summary>
    /// <param name="storage">Хранилище заголовков входящего сообщения.</param>
    /// <returns>Конфигуратор отправителя с установленным хранилище заголовков входящего сообщения.</returns>
    public RabbitSenderConfiguration WithIncomingMessageHeaderStorage(IIncomingMessageHeaderStorage storage)
    {
        Options.IncomingMessageHeaderStorage = storage;
        return this;
    }

    public RabbitSenderConfiguration WithFailoverAttempts(int failoverAttempts)
    {
        if (failoverAttempts < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(failoverAttempts), "Failover attempts must be non-negative");
        }

        Options.FailoverAttempts = failoverAttempts;
        return this;
    }
}
