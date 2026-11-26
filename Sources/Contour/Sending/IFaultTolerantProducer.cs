using System.Threading.Tasks;

namespace Contour.Sending;

internal interface IFaultTolerantProducer
{
    Task<EmitResult> Send<TRequest>(Message<TRequest> message, PublishingOptions options) where TRequest : class;

    Task<RequestResult<TResponse>> Request<TRequest, TResponse>(Message<TRequest> message, RequestOptions options) where TRequest : class where TResponse : class;
}