using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Contour.Receiving.Consumers;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Receiving;

internal abstract class AbstractReceiver<TConfiguration, TBus, TReceivingSource> : IReceiver
    where TConfiguration : IReceiverConfiguration
    where TBus : IBus
    where TReceivingSource : IReceivingSource
{
    protected readonly ILogger Logger;
    private int activeRequests;
    private bool isConfigured;
    private bool isStarted;

    public TConfiguration Configuration { get; }

    protected TBus Bus { get; }

    public abstract IEnumerable<TReceivingSource> ReceivingSources { get; }

    public int ActiveRequestsCount => this.activeRequests;

    public virtual bool IsHealthy => this.isStarted;

    protected AbstractReceiver(TConfiguration configuration, TBus bus)
    {
        Configuration = configuration;
        Bus = bus;
        this.Logger = LogManager.GetLogger($"{GetType().FullName}({Bus.Endpoint}, {Configuration.Label})");
    }

    public void RegisterConsumer<T>(IConsumer<T> consumer) where T : class
    {
        this.Logger.LogTrace("Registering consumer of [{TypeName}] in receiver of label [{Label}]", typeof(T).Name, Configuration.Label);

        foreach (var receivingSource in ReceivingSources)
        {
            receivingSource.RegisterConsumer(consumer);
        }
    }

    public async Task Prepare(CancellationToken cancellationToken)
    {
        if (!this.isConfigured)
        {
            await BuildReceivingSources(cancellationToken);
            this.isConfigured = true;
        }
    }

    public async Task Start(CancellationToken cancellationToken)
    {
        if (!this.isConfigured || this.isStarted)
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        this.Logger.LogTrace("Starting sources in receiver of [{Label}]", Configuration.Label);
        await Task.WhenAll(ReceivingSources.Select(l => l.StartConsuming(cancellationToken)));
        this.isStarted = true;
    }

    public async Task Stop(CancellationToken cancellationToken)
    {
        this.Logger.LogTrace("Stopping receiver of [{Label}]", Configuration.Label);

        await StopReceivingSources(cancellationToken);
        this.isStarted = false;
        this.isConfigured = false;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var receivingSource in ReceivingSources)
        {
            UnsubscribeFromReceivingSourceEvents(receivingSource);
            await receivingSource.DisposeAsync();
        }
        ClearReceivingSources();
    }

    private async Task StopReceivingSources(CancellationToken token)
    {
        this.Logger.LogTrace("Stopping ReceivingSource of [{Label}]", Configuration.Label);

        foreach (var receivingSource in ReceivingSources)
        {
            token.ThrowIfCancellationRequested();
            try
            {
                await receivingSource.StopConsuming(token);
            }
            catch (Exception ex)
            {
                this.Logger.LogError(ex, "Failed to stop a ReceivingSource [{ReceivingSource}] in receiver of [{Label}] due to {Message}",
                    receivingSource, Configuration.Label, ex.Message);
            }
        }
    }

    protected abstract Task BuildReceivingSources(CancellationToken cancellationToken);

    protected abstract void ClearReceivingSources();

    protected void SubscribeToSourceEvents(TReceivingSource receivingSource)
    {
        receivingSource.OnStartConsuming += OnStartedConsuming;
        receivingSource.OnFinishConsuming += OnFinishConsuming;
    }

    private void UnsubscribeFromReceivingSourceEvents(TReceivingSource receivingSource)
    {
        receivingSource.OnStartConsuming -= OnStartedConsuming;
        receivingSource.OnFinishConsuming -= OnFinishConsuming;
    }

    private void OnStartedConsuming(IReceivingSource receivingSource)
    {
        Interlocked.Increment(ref this.activeRequests);
    }

    private void OnFinishConsuming(IReceivingSource receivingSource)
    {
        Interlocked.Decrement(ref this.activeRequests);
        if (this.activeRequests < 0)
        {
            this.Logger.LogWarning("Active requests are less than zero. Set it to 0");
            this.activeRequests = 0;
        }
    }
}