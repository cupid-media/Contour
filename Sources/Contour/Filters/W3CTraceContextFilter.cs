using System;
using System.Threading.Tasks;
using Contour.Tracing;

namespace Contour.Filters
{
    /// <summary>
    /// Message exchange filter that handles W3C trace context header propagation and Activity management
    /// </summary>
    public class W3CTraceContextFilter : IMessageExchangeFilter
    {
        public async Task<MessageExchange> Process(MessageExchange exchange, MessageExchangeFilterInvoker invoker, string connectionKey)
        {
            // Start producer activity for outgoing messages
            if (exchange.Out != null)
            {
                exchange.ActivityManager.StartProducerActivity(
                    exchange.In, exchange.Out, 
                    $"Send message to {exchange.Out.Label.Name}");
                
                // Inject trace context into outgoing message headers
                if (exchange.Out.Headers != null)
                {
                    W3CTraceContextProvider.InjectTraceContext(exchange.Out.Headers, exchange.In);
                }
            }

            try
            {
                // Continue with filter chain
                var result = await invoker.Continue(exchange, connectionKey);
                
                // Complete activities with success status
                exchange.ActivityManager.CompleteConsumerActivity();
                exchange.ActivityManager.CompleteProducerActivity();
                
                return result;
            }
            catch (Exception ex)
            {
                // Set error status on activities before completing them
                if (exchange.In != null)
                {
                    exchange.ActivityManager.SetConsumerActivityError(ex);
                    exchange.ActivityManager.CompleteConsumerActivity();
                }
                
                if (exchange.Out != null)
                {
                    exchange.ActivityManager.SetProducerActivityError(ex);
                    exchange.ActivityManager.CompleteProducerActivity();
                }
                
                throw;
            }
        }

        public async Task<MessageExchange> Process(MessageExchange exchange, MessageExchangeFilterInvoker invoker)
        {
            return await Process(exchange, invoker, null);
        }
    }
}