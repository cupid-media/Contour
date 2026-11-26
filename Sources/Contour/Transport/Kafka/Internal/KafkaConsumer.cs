using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Transport.Kafka.Configuration;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.Kafka.Internal;

internal sealed class KafkaConsumer(KafkaReceiverOptions receiverOptions, IKafkaConsumerFactory consumerFactory)
{
    private static readonly ILogger Logger = LogManager.GetLogger<KafkaConsumer>();

    private Task worker;

    private CancellationTokenSource stopTokenSource;

    private bool IsConsuming { get; set; }

    public bool IsAlive => IsConsuming && Volatile.Read(ref this.worker) is { IsCompleted: false };

    public Task StartConsuming(KafkaMessageHandler handler, CancellationToken cancellationToken)
    {
        if (IsConsuming)
        {
            return Task.CompletedTask;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var loop = new KafkaConsumeLoop(receiverOptions, consumerFactory, handler);
        loop.Prepare();

        var tokenSource = new CancellationTokenSource();
        var token = tokenSource.Token;

        // Выделенный поток: Consume блокирующий.
        var newWorker = Task.Factory.StartNew(
                () => loop.Run(token),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default)
            .Unwrap();

        this.stopTokenSource = tokenSource;
        Volatile.Write(ref this.worker, newWorker);
        IsConsuming = true;

        return Task.CompletedTask;
    }

    public async Task StopConsuming(CancellationToken cancellationToken)
    {
        if (!IsConsuming)
        {
            return;
        }

        IsConsuming = false;
        this.stopTokenSource.Cancel();

        var workerToWait = this.worker;
        var tokenSourceToDispose = this.stopTokenSource;
        Volatile.Write(ref this.worker, null);
        this.stopTokenSource = null;

        try
        {
            await workerToWait.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            Logger.LogWarning("Graceful stop of consumer of topic {Label} was canceled before the worker completed", receiverOptions.Label);
            throw;
        }
        finally
        {
            tokenSourceToDispose.Dispose();
        }
    }
}
