using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Contour.Sending;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Отправка с повторами, которая перед выбором продюсера пересобирает сломанных фатальной
///   ошибкой брокера. Селектор неисправных не выдаёт, и без этого маршрут с единственным
///   продюсером — а в Kafka он всегда такой — остался бы сломанным навсегда.
/// </summary>
internal sealed class KafkaFaultTolerantProducer(
    IReadOnlyCollection<KafkaProducer> producers,
    IFaultTolerantProducer inner) : IFaultTolerantProducer
{
    private static readonly ILogger Logger = LogManager.GetLogger<KafkaFaultTolerantProducer>();

    public async Task<EmitResult> Send<TRequest>(Message<TRequest> message, PublishingOptions options)
        where TRequest : class
    {
        await RecoverProducers();

        return await inner.Send(message, options);
    }

    public async Task<RequestResult<TResponse>> Request<TRequest, TResponse>(Message<TRequest> message, RequestOptions options)
        where TRequest : class
        where TResponse : class
    {
        await RecoverProducers();

        return await inner.Request<TRequest, TResponse>(message, options);
    }

    private async Task RecoverProducers()
    {
        foreach (var producer in producers.Where(p => !p.IsInGoodCondition))
        {
            try
            {
                await producer.Recover();
            }
            catch (Exception ex)
            {
                // Пересобрать не удалось: отправку разберёт обычный механизм выбора и повторов.
                Logger.LogError(ex, "Failed to rebuild producer of [{Label}] due to {Message}", producer.Label, ex.Message);
            }
        }
    }
}
