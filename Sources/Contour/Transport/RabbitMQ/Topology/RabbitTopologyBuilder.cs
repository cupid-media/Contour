using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;
using Contour.Topology;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Transport.RabbitMQ.Topology;

/// <summary>
/// The topology builder.
/// </summary>
public class RabbitTopologyBuilder : ITopologyBuilder
{
    private readonly IRabbitConnection channelProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitTopologyBuilder"/> class.
    /// </summary>
    /// <param name="channelProvider">
    /// The provider.
    /// </param>
    internal RabbitTopologyBuilder(IRabbitConnection channelProvider)
    {
        this.channelProvider = channelProvider;
    }

    /// <summary>
    /// Привязывает точку обмена (маршрутизации) с очередью.
    /// </summary>
    /// <param name="exchange">
    /// Точка обмена (маршрутизации) сообщений, на которую поступают сообщения. Точка обмена действует на основе ключа маршрутизации <paramref name="routingKey"/>.
    /// </param>
    /// <param name="queue">
    /// Очередь, в которую будут поступать сообщения из маршрутизатора.
    /// </param>
    /// <param name="routingKey">
    /// Ключ маршрутизации, используется для определения очереди, в которую должно быть отправлено сообщение.
    /// </param>
    public async Task Bind(Exchange exchange, Queue queue, string routingKey = "", CancellationToken cancellationToken = default)
    {
        await using var channel = await this.channelProvider.OpenChannel(token: cancellationToken);
        await channel.Bind(queue, exchange, routingKey, cancellationToken);
    }

    /// <summary>
    /// Создает временную конечную точку подписки на сообщения.
    /// Обычно используется для организации варианта коммуникации: запрос-ответ.
    /// </summary>
    /// <returns>
    /// Конечная точка подписки на сообщения <see cref="ISubscriptionEndpoint"/>.
    /// </returns>
    public async Task<ISubscriptionEndpoint> BuildTempReplyEndpoint(CancellationToken cancellationToken)
    {
        await using var channel = await this.channelProvider.OpenChannel(token: cancellationToken);
        var queue = new Queue(await channel.DeclareDefaultQueue(cancellationToken));
        return new SubscriptionEndpoint(queue, new StaticRouteResolver(string.Empty, queue.Name));
    }

    /// <summary>
    /// Создает временную конечную точку для получения сообщений.
    /// </summary>
    /// <param name="endpoint">Конечная точка шины сообщений для который создается подписка.</param>
    /// <param name="label">Метка сообщений, на которые ожидается получение ответа.</param>
    /// <returns>
    /// Конечная точка подписки для получения сообщений.
    /// </returns>
    public async Task<ISubscriptionEndpoint> BuildTempReplyEndpoint(IEndpoint endpoint, MessageLabel label, CancellationToken cancellationToken)
    {
        await using var channel = await this.channelProvider.OpenChannel(token: cancellationToken);
        var queue =
            Queue.Named(
                    $"{endpoint.Address}.replies-{(label.IsAny ? "any" : label.Name)}-{NameGenerator.GetRandomName(8)}")
                .WithAutoDelete().Exclusive().Instance;

        await channel.Declare(queue, cancellationToken);

        return new SubscriptionEndpoint(queue, new StaticRouteResolver(string.Empty, queue.Name));
    }

    /// <summary>
    /// Создает точку обмена (маршрутизации), на которую поступают сообщения в брокере.
    /// </summary>
    /// <param name="builder">
    /// Построитель точки обмена (маршрутизации).
    /// </param>
    /// <returns>
    /// Точка обмена (маршрутизации) <see cref="Exchange"/>.
    /// </returns>
    public async Task<Exchange> Declare(ExchangeBuilder builder, CancellationToken cancellationToken)
    {
        await using var channel = await this.channelProvider.OpenChannel(token: cancellationToken);
        var exchange = builder.Instance;
        await channel.Declare(exchange, cancellationToken);
        return exchange;
    }

    /// <summary>
    /// Создает очередь в брокере для всех сообщений готовых к обработке.
    /// </summary>
    /// <param name="builder">
    /// Построитель очереди.
    /// </param>
    /// <returns>
    /// Очередь сообщений <see cref="Queue"/> для обработки.
    /// </returns>
    public async Task<Queue> Declare(QueueBuilder builder, CancellationToken cancellationToken)
    {
        await using var channel = await this.channelProvider.OpenChannel(token: cancellationToken);
        var queue = builder.Instance;
        await channel.Declare(queue, cancellationToken);
        return queue;
    }

    public void Dispose()
    {
    }
}