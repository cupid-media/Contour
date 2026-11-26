using System;
using System.Threading.Tasks;

namespace Contour.Receiving;

internal class MessageExpectation(Func<IDelivery, IMessage> responseBuilderFunc, long? timeoutTicket)
{
    private readonly TaskCompletionSource<IMessage> completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<IMessage> Task => this.completionSource.Task;

    public long? TimeoutTicket { get; private set; } = timeoutTicket;

    public void Cancel()
    {
        this.completionSource.TrySetException(new OperationCanceledException());
    }

    public void Complete(IDelivery delivery)
    {
        try
        {
            var response = responseBuilderFunc(delivery);
            this.completionSource.TrySetResult(response);
        }
        catch (Exception ex)
        {
            this.completionSource.TrySetException(ex);
            throw;
        }
    }

    public void Timeout()
    {
        this.completionSource.TrySetException(new TimeoutException());
    }
}
