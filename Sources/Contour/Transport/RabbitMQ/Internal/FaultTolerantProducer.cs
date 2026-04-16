using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Logging;

namespace Contour.Transport.RabbitMQ.Internal
{
    internal class FaultTolerantProducer : IFaultTolerantProducer
    {
        private readonly ILog logger = LogManager.GetLogger<FaultTolerantProducer>();
        private readonly IProducerSelector selector;
        private readonly int attempts;

        private bool disposed;

        public FaultTolerantProducer(IProducerSelector selector, int maxAttempts, int maxRetryDelay, int inactivityResetDelay)
        {
            this.selector = selector ?? throw new ArgumentNullException(nameof(selector));
            this.attempts = maxAttempts;
        }

        public Task<MessageExchange> Send(MessageExchange exchange, string connectionKey)
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(typeof(FaultTolerantProducer).Name);
            }

            var errors = new List<Exception>();

            for (var count = 0; count < this.attempts; count++)
            {
                try
                {
                    var producer = connectionKey == null ? this.selector.Next() : this.selector.PickByConnectionKey(connectionKey);
                    this.logger.Info($"Send attempt #{count + 1}/{this.attempts}: label=[{exchange.Out?.Label}], producer=[{producer.BrokerUrl}], isRequest={exchange.IsRequest}, connectionKey=[{connectionKey}]");
                    return this.TrySend(exchange, producer);
                }
                catch (Exception ex)
                {
                    this.logger.Warn($"Send attempt #{count + 1}/{this.attempts} FAILED for label=[{exchange.Out?.Label}], connectionKey=[{connectionKey}]: {ex.Message}", ex);
                    errors.Add(ex);
                }
            }

            this.logger.Error($"All {this.attempts} send attempts exhausted for label=[{exchange.Out?.Label}], connectionKey=[{connectionKey}]");
            throw new FailoverException($"Failed to send a message after {this.attempts} attempts", new AggregateException(errors))
            {
                Attempts = this.attempts
            };
        }

        private Task<MessageExchange> TrySend(MessageExchange exchange, IProducer producer)
        {
            if (exchange.IsRequest)
            {
                return producer.Request(exchange.Out, exchange.ExpectedResponseType)
                    .ContinueWith(
                        t =>
                        {
                            if (t.IsFaulted)
                            {
                                exchange.Exception = t.Exception;
                            }
                            else
                            {
                                exchange.In = t.Result;
                            }

                            return exchange;
                        });
            }

            return producer.Publish(exchange.Out)
                .ContinueWith(
                    t =>
                    {
                        if (t.IsFaulted)
                        {
                            exchange.Exception = t.Exception;
                        }

                        return exchange;
                    });
        }

        /// <summary>
        /// Âûïîëíÿåò îïðåäåëÿåìûå ïðèëîæåíèåì çàäà÷è, ñâÿçàííûå ñ óäàëåíèåì, âûñâîáîæäåíèåì èëè ñáðîñîì íåóïðàâëÿåìûõ ðåñóðñîâ.
        /// </summary>
        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.disposed = true;
        }
    }
}