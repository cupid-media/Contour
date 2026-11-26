using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Contour.Sending.Exceptions;
using Microsoft.Extensions.Logging;

namespace Contour.Sending;

internal class FaultTolerantProducer(IProducerSelector selector, int maxAttempts) : IFaultTolerantProducer
{
    private readonly ILogger logger = LogManager.GetLogger<FaultTolerantProducer>();
    private readonly IProducerSelector selector = selector ?? throw new ArgumentNullException(nameof(selector));

    public async Task<EmitResult> Send<TRequest>(Message<TRequest> message, PublishingOptions options) where TRequest : class
    {
        return await SendInternal(async producer => await producer.Send(message), options.ForceConnectionKey);
    }

    public async Task<RequestResult<TResponse>> Request<TRequest, TResponse>(Message<TRequest> message, RequestOptions options) where TRequest : class where TResponse : class
    {
        return await SendInternal(async producer => await producer.Request<TRequest, TResponse>(message), options.ForceConnectionKey);
    }

    private async Task<TResponse> SendInternal<TResponse>(Func<IProducer, Task<TResponse>> action, string connectionKey)
    {
        var errors = new List<Exception>();

        for (var count = 0; count < maxAttempts; count++)
        {
            this.logger.LogTrace("Attempt to send #{Count}", count);

            try
            {
                var producer = connectionKey == null ? this.selector.Next() : this.selector.PickByConnectionKey(connectionKey);
                return await action(producer);
            }
            catch (FailedToSendMessageException ex)
            {
                var inner = ex.InnerException ?? ex;
                this.logger.LogWarning(inner, "Attempt #{Count} to send a message has failed", count);
                errors.Add(inner);
            }
        }

        throw new FailoverException($"Failed to send a message after {maxAttempts} attempts", new AggregateException(errors))
        {
            Attempts = maxAttempts
        };
    }
}
