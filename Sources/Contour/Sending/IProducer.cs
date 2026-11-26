using System;
using System.Threading;
using System.Threading.Tasks;

namespace Contour.Sending;

internal interface IProducer : IAsyncDisposable
{
    string ConnectionKey { get; }

    Task<EmitResult> Send<TRequest>(Message<TRequest> message) where TRequest : class;

    Task<RequestResult<TResponse>> Request<TRequest, TResponse>(Message<TRequest> request) where TRequest : class where TResponse : class;

    Task Prepare(CancellationToken token);

    Task Start(CancellationToken token);

    Task Stop(CancellationToken token);

    bool IsInGoodCondition { get; }
}
