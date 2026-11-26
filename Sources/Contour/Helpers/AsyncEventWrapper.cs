using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace Contour.Helpers;

internal class AsyncEventWrapper<TEvent> where TEvent : AsyncEventArgs
{
    private static readonly ILogger Logger = LogManager.GetLogger<AsyncEventWrapper<TEvent>>();

    public event Func<TEvent, Task> Event;

    public async Task Invoke(TEvent @event)
    {
        if (Event is null)
        {
            return;
        }

        var invocationList = Event.GetInvocationList();
        var tasks = invocationList.Select(async @delegate =>
        {
            var action = (Func<TEvent, Task>)@delegate;
            try
            {
                await action(@event);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error occured while invoking event");
            }
        });

        await Task.WhenAll(tasks);
    }
}
