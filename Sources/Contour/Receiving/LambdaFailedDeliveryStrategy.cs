using System.Threading.Tasks;
using System;
using Microsoft.Extensions.Logging;

namespace Contour.Receiving;

public class LambdaFailedDeliveryStrategy(Func<IFailedConsumingContext, Task> handlerAction) : IFailedDeliveryStrategy
{
    private static readonly ILogger Log = LogManager.GetLogger<LambdaFailedDeliveryStrategy>();

    public async Task Handle(IFailedConsumingContext failedConsumingContext)
    {
        try
        {
            await handlerAction(failedConsumingContext);
        }
        catch (Exception ex)
        {
            Log.LogError(ex, "Unable to handle failed message [{Label}]", failedConsumingContext.Delivery.Label);
        }
    }
}
