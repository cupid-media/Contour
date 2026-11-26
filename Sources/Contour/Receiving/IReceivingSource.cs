using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving.Consumers;

namespace Contour.Receiving;

internal interface IReceivingSource : IAsyncDisposable
{
    event Action<IReceivingSource> OnStartConsuming;

    event Action<IReceivingSource> OnFinishConsuming;

    void RegisterConsumer<T>(IConsumer<T> consumer) where T : class;

    Task Prepare(CancellationToken cancellationToken);

    Task StartConsuming(CancellationToken cancellationToken);

    Task StopConsuming(CancellationToken cancellationToken);
}
