using System;
using System.Diagnostics;

namespace Contour.Tracing
{
    /// <summary>
    /// Manages Activity lifecycle for message consuming and producing operations
    /// </summary>
    public class ActivityManager : IDisposable
    {
        private Activity consumerActivity;
        private Activity producerActivity;
        private bool disposed;

        /// <summary>
        /// Starts a consumer activity for processing incoming messages
        /// </summary>
        /// <param name="message">The incoming message to extract trace context from</param>
        /// <param name="operationName">The operation name for the activity</param>
        /// <returns>The started consumer activity or null if not enabled</returns>
        public void StartConsumerActivity(IMessage message, string operationName = "consume")
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(ActivityManager));

            // Dispose existing consumer activity if any
            consumerActivity?.Dispose();

            consumerActivity = message.StartActivityWithMessageContext(operationName, ActivityKind.Consumer);
            
            // Add common tags for consumer activities
            if (consumerActivity != null)
            {
                consumerActivity.SetTag("messaging.operation", "consume");
                consumerActivity.SetTag("messaging.system", "contour");
                
                if (message.Label != null)
                {
                    consumerActivity.SetTag("messaging.queue", message.Label.Name);
                }
            }
        }

        /// <summary>
        /// Starts a producer activity for sending outgoing messages
        /// </summary>
        /// <param name="parentMessage">Parent message</param>
        /// <param name="producedMessage">The outgoing message</param>
        /// <param name="operationName">The operation name for the activity</param>
        /// <returns>The started producer activity or null if not enabled</returns>
        public Activity StartProducerActivity(IMessage producedMessage, string operationName = "produce")
        {
            if (disposed)
                throw new ObjectDisposedException(nameof(ActivityManager));

            producerActivity?.Dispose();

            producerActivity = ContourActivitySource.Source.StartActivity(operationName, ActivityKind.Producer);
            
            if (producerActivity != null)
            {
                producerActivity.SetTag("messaging.operation", "produce");
                producerActivity.SetTag("messaging.system", "contour");
                
                if (producedMessage.Label != null)
                {
                    producerActivity.SetTag("messaging.queue", producedMessage.Label.Name);
                }
            }

            return producerActivity;
        }

        /// <summary>
        /// Completes the consumer
        /// </summary>
        public void CompleteConsumerActivity()
        {
            if (consumerActivity == null) 
                return;
            
            if (consumerActivity.Status == ActivityStatusCode.Unset)
                consumerActivity.SetStatus(ActivityStatusCode.Ok);
                
            consumerActivity.Dispose();
            consumerActivity = null;
        }

        /// <summary>
        /// Completes the producer activity
        /// </summary>
        public void CompleteProducerActivity()
        {
            if (producerActivity == null) 
                return;
            
            if (producerActivity.Status == ActivityStatusCode.Unset)
                producerActivity.SetStatus(ActivityStatusCode.Ok);
                
            producerActivity.Dispose();
            producerActivity = null;
        }

        /// <summary>
        /// Sets an error status on the consumer activity
        /// </summary>
        /// <param name="exception">The exception that occurred</param>
        public void SetConsumerActivityError(Exception exception)
        {
            if (consumerActivity != null)
            {
                consumerActivity.SetStatus(ActivityStatusCode.Error, exception.Message);
                consumerActivity.SetTag("error", true);
                consumerActivity.SetTag("error.type", exception.GetType().Name);
                consumerActivity.SetTag("error.message", exception.Message);
            }
        }

        /// <summary>
        /// Sets an error status on the producer activity
        /// </summary>
        /// <param name="exception">The exception that occurred</param>
        public void SetProducerActivityError(Exception exception)
        {
            if (producerActivity != null)
            {
                producerActivity.SetStatus(ActivityStatusCode.Error, exception.Message);
                producerActivity.SetTag("error", true);
                producerActivity.SetTag("error.type", exception.GetType().Name);
                producerActivity.SetTag("error.message", exception.Message);
            }
        }

        /// <summary>
        /// Disposes all managed activities
        /// </summary>
        public void Dispose()
        {
            if (disposed) 
                return;
            
            consumerActivity?.Dispose();
            producerActivity?.Dispose();
            consumerActivity = null;
            producerActivity = null;
            disposed = true;
        }
    }
}
