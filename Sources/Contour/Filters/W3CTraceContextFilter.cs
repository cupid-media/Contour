using System.Collections.Generic;
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
            if (exchange.In != null)
            {
                // TODO: check if it will be disposed.
                // if not - what can we do? maybe move this out of filter?
                exchange.In?.StartActivityWithMessageContext();
            }

            
            // For outgoing messages, copy trace context from current activity,
            // or incoming message if available 
            if (exchange.Out?.Headers != null)
            {
                // TODO: start producer activity
                exchange.Out?.StartActivityWithMessageContext();
                W3CTraceContextProvider.InjectTraceContext(exchange.Out.Headers, exchange.In);
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