using System.Threading.Tasks;
using Contour.Tracing;

namespace Contour.Filters
{
    /// <summary>
    /// Message exchange filter that handles W3C trace context header propagation
    /// </summary>
    public class W3CTraceContextFilter : IMessageExchangeFilter
    {
        public async Task<MessageExchange> Process(MessageExchange exchange, MessageExchangeFilterInvoker invoker, string connectionKey)
        {
            // For outgoing messages, copy trace context from incoming message if available
            if (exchange.Out?.Headers != null && exchange.In != null)
            {
                W3CTraceContextProvider.CopyTraceContextFromMessage(exchange.Out.Headers, exchange.In);
            }

            // Continue with filter chain - no Activity manipulation needed
            return await invoker.Continue(exchange, connectionKey);
        }

        public async Task<MessageExchange> Process(MessageExchange exchange, MessageExchangeFilterInvoker invoker)
        {
            return await Process(exchange, invoker, null);
        }
    }
}