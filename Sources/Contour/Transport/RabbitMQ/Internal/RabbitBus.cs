using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Contour.Configuration;
using Contour.Helpers;
using Contour.Receiving;
using Contour.Sending;

namespace Contour.Transport.RabbitMQ.Internal
{
    /// <summary>
    /// The rabbit bus.
    /// </summary>
    internal class RabbitBus : AbstractBus, IBusAdvanced
    {
        private readonly ILog logger = LogManager.GetLogger<RabbitBus>();
        private readonly ManualResetEvent ready = new ManualResetEvent(false);
        private readonly IConnectionPool<IRabbitConnection> connectionPool;

        /// <summary>
        /// Шина полностью сконфигурированна и готова слушать входящие и публиковать исходящии, осталось ее только включить
        /// </summary>
        private volatile bool isPrepared;

        private CancellationTokenSource cancellationTokenSource;
        private Task workTask;

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitBus" /> class.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        public RabbitBus(BusConfiguration configuration)
            : this(configuration, null)
        {
        }

        internal RabbitBus(BusConfiguration configuration, IConnectionPool<IRabbitConnection> connectionPool)
            : base(configuration)
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            var completion = new TaskCompletionSource<object>();
            completion.SetResult(new object());
            this.workTask = completion.Task;
            
            this.connectionPool = connectionPool ?? new RabbitConnectionPool(this);
        }
        
        /// <summary>
        /// Gets the when ready.
        /// </summary>
        public override WaitHandle WhenReady => this.ready;

        internal TimeSpan StopWaitTimeout { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Starts a bus
        /// </summary>
        /// <exception cref="AggregateException">Any exceptions thrown during the bus start</exception>
        public override Task Start()
        {
            if (this.IsStarted || this.IsShuttingDown)
            {
                return Task.CompletedTask;
            }

            this.logger.Trace(m => m("{0}: Starting...", this.Endpoint));

            var token = this.cancellationTokenSource.Token;
            this.workTask = Task.Factory.StartNew(this.PrepareTask, token, TaskCreationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(_ => this.StartTask(), token, TaskContinuationOptions.LongRunning, TaskScheduler.Default)
                .ContinueWith(
                    t =>
                        {
                            if (t.IsFaulted)
                            {
                                // TODO ошибка тут приводит к тому, что сервис зависает навсегда с неготовой шиной
                                // если раньше это приводило к потере сообщений, при попытке их обработать, теперь сервис просто ничего делать не будет
                                this.logger.Error(m => m("Error on restarting bus: {0}", this.Endpoint.Address), t.Exception.InnerException);
                                throw t.Exception.InnerException;
                            }
                        });
            
            return this.workTask;
        }

        /// <summary>
        /// Prepare a bus
        /// </summary>
        /// <returns>Task </returns>
        public override Task Prepare()
        {
            var token = this.cancellationTokenSource.Token;
            return Task.Factory.StartNew(this.PrepareTask, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public override void Stop()
        {
            this.ResetWorkTask();

            var token = this.cancellationTokenSource.Token;
            this.workTask = Task.Factory.StartNew(this.StopTask, token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            this.workTask.Wait(this.StopWaitTimeout);
        }

        /// <summary>
        /// Shuts the bus down
        /// </summary>
        public override void Shutdown()
        {
            this.logger.InfoFormat(
                "Shutting down [{0}] with endpoint [{1}].", 
                this.GetType().Name, 
                this.Endpoint);

            this.IsShuttingDown = true;

            try
            {
                this.Stop();

                // если не ожидать завершения задачи до сброса флага IsShuttingDown и конфигурации,
                // Stop() вернется через 5 секунд, пока StopTask еще дренирует in-flight обработчики;
                // также при ошибках задача могла не успеть закрыться и входить в бесконечное ожидание в Restart -> ResetRestartTask.
                this.workTask.Wait();

                this.logger.Trace(m => m("{0}: resetting bus configuration", this.Endpoint));
                this.IsConfigured = false;
                this.isPrepared = false;

                this.ComponentTracker.UnregisterAll();
                this.connectionPool.Drop();
            }
            finally
            {
                this.IsShuttingDown = false;
            }
        }

        /// <summary>
        /// Registers a receiver using the <paramref name="configuration"/>
        /// </summary>
        /// <param name="configuration">
        /// Receiver configuration
        /// </param>
        /// <param name="isCallback">
        /// Denotes if a receiver should handle the callback messages
        /// </param>
        /// <returns>
        /// The <see cref="RabbitReceiver"/>.
        /// </returns>
        public RabbitReceiver RegisterReceiver(IReceiverConfiguration configuration, bool isCallback = false)
        {
            this.logger.Trace(
                $"Registering a new receiver of [{configuration.Label}] with connection string [{configuration.Options.GetConnectionString()}]");

            RabbitReceiver receiver;
            if (isCallback)
            {
                receiver = new RabbitCallbackReceiver(this, configuration, this.connectionPool);

                // Callback receivers are owned by their Producer/RabbitSender lifecycle and must not be added to ComponentTracker
                // (otherwise IsHealthy remains false and Stop()->Start() or producer reconnects accumulate duplicate instances).
            }
            else
            {
                receiver = new RabbitReceiver(this, configuration, this.connectionPool);
                receiver.ListenerCreated += this.OnListenerCreated;
                this.ComponentTracker.Register(receiver);
            }

            this.logger.Trace(
                $"A receiver of [{configuration.Label}] with connection string [{configuration.Options.GetConnectionString()}] registered successfully");
            return receiver;
        }

        /// <summary>
        /// Registers a sender using <paramref name="configuration"/>
        /// </summary>
        /// <param name="configuration">
        /// Sender configuration
        /// </param>
        /// <returns>
        /// The <see cref="RabbitSender"/>.
        /// </returns>
        public RabbitSender RegisterSender(ISenderConfiguration configuration)
        {
            this.logger.Trace(
                $"Registering a new sender of [{configuration.Label}] with connection string [{configuration.Options.GetConnectionString()}]");

            var sender = new RabbitSender(this, configuration, this.connectionPool, this.Configuration.Filters.ToList());
            this.ComponentTracker.Register(sender);

            this.logger.Trace(
                $"A sender of [{configuration.Label}] with connection string [{configuration.Options.GetConnectionString()}] registered successfully");

            return sender;
        }

        private void StartTask()
        {
            if (this.IsShuttingDown)
            {
                return;
            }

            this.logger.Trace(m => m("{0}: marking as ready.", this.Endpoint));
            this.ready.Set();
            this.IsStarted = true;
            
            this.OnStarted();
        }

        private void PrepareTask()
        {
            if (this.IsShuttingDown)
            {
                return;
            }

            if (this.isPrepared)
            {
                return;
            }

            this.OnStarting();

            this.logger.Trace(m => m("{0}: configuring.", this.Endpoint));
            this.Configure();

            this.logger.Trace(m => m("{0}: starting components.", this.Endpoint));
            this.ComponentTracker.StartAll();

            this.isPrepared = true;
        }

        private void StopTask()
        {
            if (!this.IsConfigured)
            {
                return;
            }

            this.OnStopping();

            this.logger.Trace(m => m("{0}: stopping regular receivers.", this.Endpoint));
            var regularReceivers = this.Receivers
                .Where(r => !(r is RabbitCallbackReceiver))
                .ToArray();

            if (regularReceivers.Length > 0)
            {
                Task.WaitAll(regularReceivers.Select(r => Task.Run(() => r.Stop())).ToArray());
            }

            this.logger.Trace(m => m("{0}: marking as not ready.", this.Endpoint));
            this.ready.Reset();

            this.logger.Trace(m => m("{0}: stopping senders.", this.Endpoint));
            this.Senders.ForEach(s => s.Stop());

            this.logger.Trace(m => m("{0}: stopping remaining bus components.", this.Endpoint));
            this.ComponentTracker.StopAll();

            this.isPrepared = false;
            this.OnStopped();
        }

        private void ResetWorkTask()
        {
            if (!this.workTask.IsCompleted)
            {
                this.cancellationTokenSource.Cancel();
                try
                {
                    this.workTask.Wait();
                }
                catch (AggregateException ex)
                {
                    ex.Handle(
                        e =>
                        {
                            this.logger.ErrorFormat("{0}: Caught unexpected exception.", e, this.Endpoint);
                            return true;
                        });
                }
                catch (Exception ex)
                {
                    this.logger.ErrorFormat("{0}: Caught unexpected exception.", ex, this.Endpoint);
                }
                finally
                {
                    this.cancellationTokenSource = new CancellationTokenSource();
                }
            }
        }

        private void Configure()
        {
            if (this.IsConfigured)
            {
                return;
            }

            var name = this.GetType().Name;
            var senderConfigurations = this.Configuration.SenderConfigurations.ToList();
            var receiverConfigurations = this.Configuration.ReceiverConfigurations.ToList();

            this.logger.Trace(
                $"Configuring [{name}] with endpoint [{this.Endpoint}]:\r\nSenders:\r\n\t{string.Join("\r\n\t", senderConfigurations.Select(s => $"[{s.Label}]\t=>\t{s.Options.GetConnectionString()}"))}\r\nReceivers:\r\n\t{string.Join("\r\n\t", receiverConfigurations.Select(r => $"[{r.Label}]\t=>\t{r.Options.GetConnectionString()}"))}");

            foreach (var sender in senderConfigurations)
            {
                this.RegisterSender(sender);
            }

            foreach (var receiver in receiverConfigurations)
            {
                this.RegisterReceiver(receiver);
            }

            this.IsConfigured = true;
            this.logger.Info($"Configuration of [{name}] completed successfully");
        }
        
        private void OnListenerCreated(object sender, ListenerCreatedEventArgs e)
        {
            this.Receivers
                .Where(r => r is RabbitReceiver)
                .Cast<RabbitReceiver>()
                .ForEach(r => r.CheckIfCompatible(e.Listener));
        }
    }
}
