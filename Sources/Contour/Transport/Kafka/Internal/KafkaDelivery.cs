using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Serialization;
using Contour.Transport.Kafka.Configuration;

namespace Contour.Transport.Kafka.Internal;

internal class KafkaDelivery : IDelivery
{
    /// <summary>
    ///   Заголовки, относящиеся к транспорту исходного сообщения: в пересылаемое не переносятся.
    /// </summary>
    private static readonly string[] NotForwardedHeaders = [Contour.Headers.ReplyRoute, Contour.Headers.Timeout];

    private readonly KafkaBus bus;

    /// <summary>
    ///   Кластер, из которого прочитано сообщение: ответы и пересылки уходят в него же.
    /// </summary>
    private readonly string connectionString;

    /// <summary>
    ///   Решение о продвижении оффсета, если обработчик выносит его после возврата управления.
    ///   Батчевые обработчики именно так и устроены: они складывают сообщение в накопитель,
    ///   а подтверждают всю пачку позже.
    /// </summary>
    private readonly TaskCompletionSource<bool> acknowledgement =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal KafkaDelivery(
        KafkaBus bus,
        string connectionString,
        string topic,
        byte[] payload,
        IDictionary<string, object> headers,
        IPayloadConverter payloadConverter)
    {
        this.bus = bus;
        this.connectionString = connectionString;

        Topic = topic;
        Payload = payload;
        Headers = headers ?? new Dictionary<string, object>();
        PayloadConverter = payloadConverter;
    }

    public string Topic { get; }

    /// <summary>
    ///   Полезная нагрузка до десериализации.
    /// </summary>
    public byte[] Payload { get; }

    public IDictionary<string, object> Headers { get; }

    public IPayloadConverter PayloadConverter { get; }

    public MessageLabel Label => MessageLabel.From(Topic);

    /// <summary>
    ///   Ответить можно, если у шины есть продюсер, а у доставки — и топик ответов,
    ///   и корреляционный идентификатор.
    /// </summary>
    public bool CanReply => ReplySender != null
                            && !string.IsNullOrWhiteSpace(ReplyTopic)
                            && !string.IsNullOrWhiteSpace(CorrelationId);

    /// <summary>
    ///   Корреляционный идентификатор: по нему ответ сопоставляется со своим запросом.
    /// </summary>
    internal string CorrelationId => Contour.Headers.GetString(Headers, Contour.Headers.CorrelationId);

    /// <summary>
    ///   Решение обработчика о продвижении оффсета: <c>null</c> — не высказано.
    /// </summary>
    internal bool? Acknowledged { get; private set; }

    private string ReplyTopic => Contour.Headers.GetString(Headers, Contour.Headers.ReplyRoute);

    private KafkaSender ReplySender => this.bus?.GetReplySender(this.connectionString);

    private string EndpointAddress => this.bus?.Endpoint.Address;

    public Task AcceptAsync(CancellationToken cancellationToken = default)
    {
        Acknowledged = true;
        this.acknowledgement.TrySetResult(true);

        return Task.CompletedTask;
    }

    /// <summary>
    ///   Помечает сообщение как необработанное.
    /// </summary>
    /// <param name="requeue">
    ///   <c>true</c> — оффсет не продвигается, и сообщение будет прочитано снова после перезапуска
    ///   или ребаланса. В Kafka нет возврата одного сообщения в очередь, поэтому вместе с ним
    ///   перечитываются и все следующие за ним в этой партиции.
    ///   <c>false</c> — сообщение пропускается: оффсет продвигается, как при успешной обработке.
    /// </param>
    /// <param name="cancellationToken">Не используется: решение принимается локально.</param>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default)
    {
        Acknowledged = !requeue;
        this.acknowledgement.TrySetResult(!requeue);

        return Task.CompletedTask;
    }

    /// <summary>
    ///   Пересылает сообщение, сохраняя сквозные заголовки исходного: пересланное можно
    ///   связать с исходной цепочкой.
    /// </summary>
    public Task ForwardAsync(MessageLabel label, object payload)
    {
        if (label.IsEmpty)
        {
            throw new ArgumentException("Forward topic must be specified", nameof(label));
        }

        EnsureReplySender("forward a message");

        return ReplySender.SendRaw(label, payload, BuildForwardedHeaders());
    }

    /// <summary>
    ///   Перекладывает сообщение в fault-топик
    /// </summary>
    internal Task ForwardToFaultAsync(object payload)
    {
        var faultTopic = KafkaTopic.FaultOf(EndpointAddress);
        var sender = this.bus?.GetFaultSender()
                     ?? throw new BusConfigurationException(
                         $"Cannot forward a failed message from topic [{Topic}]: no route to fault topic [{faultTopic}] is declared.");

        return sender.SendRaw(faultTopic, payload, BuildForwardedHeaders());
    }

    /// <summary>
    ///   Отсылает ответ в топик из заголовка <c>x-reply-route</c> с корреляционным
    ///   идентификатором запроса.
    /// </summary>
    /// <remarks>
    ///   Заголовок <see cref="Contour.Headers.Expires"/> отклоняется: у сообщения Kafka нет своего
    ///   времени жизни — как долго оно доступно, определяет политика хранения топика. Принять
    ///   заголовок и ничего не сделать значило бы дать ложную гарантию.
    ///   Заголовки ответа собираются явно, а не через <c>ApplyPublishingOptions</c>: ответ должен
    ///   унести корреляционный идентификатор именно этого запроса, а не тот, что лежит
    ///   в ambient-хранилище заголовков.
    /// </remarks>
    public Task ReplyWithAsync<TReply>(Message<TReply> message, CancellationToken cancellationToken = default)
        where TReply : class
    {
        if (message.Headers != null && message.Headers.ContainsKey(Contour.Headers.Expires))
        {
            throw new BusConfigurationException(
                "Kafka messages have no expiration: it is set by the retention policy of the topic. " +
                "Reply without expires.");
        }

        EnsureReplySender("reply to a message");

        var replyTopic = ReplyTopic;
        var correlationId = CorrelationId;

        if (string.IsNullOrWhiteSpace(replyTopic) || string.IsNullOrWhiteSpace(correlationId))
        {
            throw new BusConfigurationException(
                $"Cannot reply to a message from topic [{Topic}]: " +
                $"[{Contour.Headers.ReplyRoute}] or [{Contour.Headers.CorrelationId}] header is missing. " +
                "Make sure the requester uses RequestAsync.");
        }

        var headers = new Dictionary<string, object>
        {
            [Contour.Headers.CorrelationId] = correlationId
        };

        Contour.Headers.ApplyBreadcrumbs(headers, EndpointAddress);
        Contour.Headers.ApplyOriginalMessageId(headers);
        Contour.Headers.ApplySentTimestamp(headers);
        CopyTraceContext(message.Headers, headers);

        return ReplySender.SendRaw(MessageLabel.From(replyTopic), message.Payload, headers);
    }

    public Message<TResponse> UnpackAs<TResponse>()
        where TResponse : class
    {
        return ToMessage<TResponse>();
    }

    /// <summary>
    ///   Десериализует полезную нагрузку и собирает сообщение шины.
    /// </summary>
    /// <remarks>
    ///   Имя топика становится меткой сообщения. Метки в Contour приводятся к нижнему
    ///   регистру, поэтому имена топиков проверяются при конфигурировании — см. <c>KafkaTopic</c>.
    /// </remarks>
    public Message<T> ToMessage<T>()
        where T : class
    {
        return new Message<T>(Label, PayloadConverter.ToObject<T>(Payload), Headers);
    }

    /// <summary>
    ///   Ожидает решения обработчика, вынесенного после возврата управления. Отмена
    ///   <paramref name="cancellationToken"/> — это не ошибка, а ответ «подтверждения не будет»:
    ///   сообщение остаётся неподтверждённым и перечитается после перезапуска. Уже принятое
    ///   решение отмена не перебивает.
    /// </summary>
    internal ConsumingResult WhenAcknowledged(CancellationToken cancellationToken = default)
    {
        if (this.acknowledgement.Task.IsCompleted || !cancellationToken.CanBeCanceled)
        {
            return ConsumingResult.Awaited(this.acknowledgement.Task);
        }

        return ConsumingResult.Awaited(Awaited(this.acknowledgement, cancellationToken));

        static async Task<bool> Awaited(TaskCompletionSource<bool> acknowledgement, CancellationToken token)
        {
            await using var registration = token.Register(() => acknowledgement.TrySetResult(false));

            return await acknowledgement.Task;
        }
    }

    private IDictionary<string, object> BuildForwardedHeaders()
    {
        var headers = Headers
            .Where(h => !NotForwardedHeaders.Contains(h.Key))
            .ToDictionary(h => h.Key, h => h.Value);

        headers[Contour.Headers.MessageLabel] = Topic;
        headers[Contour.Headers.Forwarded] = true;

        Contour.Headers.ApplyBreadcrumbs(headers, EndpointAddress);
        Contour.Headers.ApplyOriginalMessageId(headers);
        Contour.Headers.ApplySentTimestamp(headers);

        if (!headers.ContainsKey(Contour.Headers.CorrelationId))
        {
            headers[Contour.Headers.CorrelationId] = Guid.NewGuid().ToString("n");
        }

        return headers;
    }

    /// <summary>
    ///   Переносит в заголовки ответа контекст трассировки, который обработчик положил в ответное
    ///   сообщение: остальные заголовки ответа собираются явно, см. <see cref="ReplyWithAsync{TReply}"/>.
    /// </summary>
    private static void CopyTraceContext(IDictionary<string, object> source, IDictionary<string, object> target)
    {
        if (source == null)
        {
            return;
        }

        if (source.TryGetValue(Contour.Headers.TraceParent, out var traceParent))
        {
            target[Contour.Headers.TraceParent] = traceParent;
        }

        if (source.TryGetValue(Contour.Headers.TraceState, out var traceState))
        {
            target[Contour.Headers.TraceState] = traceState;
        }
    }

    private void EnsureReplySender(string operation)
    {
        if (ReplySender == null)
        {
            throw new BusConfigurationException(
                $"Cannot {operation} from a handler of topic [{Topic}]: " +
                $"the bus has no producer for [{this.connectionString}]. Call WithReplies() on the receiver.");
        }
    }
}
