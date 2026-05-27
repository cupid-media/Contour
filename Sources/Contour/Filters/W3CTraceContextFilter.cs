using System.Threading.Tasks;
using Contour.Tracing;

namespace Contour.Filters
{
    /// <summary>
    /// Message exchange filter that handles W3C trace context header propagation.
    /// </summary>
    public class W3CTraceContextFilter : IMessageExchangeFilter
    {
        public async Task<MessageExchange> Process(MessageExchange exchange, MessageExchangeFilterInvoker invoker, string connectionKey)
        {
            if (exchange.Out?.Headers != null)
            {
                W3CTraceContextProvider.InjectTraceContext(exchange.Out.Headers, exchange.In);
            }

            return await invoker.Continue(exchange, connectionKey);
        }

        public async Task<MessageExchange> Process(MessageExchange exchange, MessageExchangeFilterInvoker invoker)
        {
            return await Process(exchange, invoker, null);
        }
    }
}