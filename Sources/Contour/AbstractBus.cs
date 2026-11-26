using Contour.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Sending;
using Contour.Serialization;
using Microsoft.Extensions.Logging;

namespace Contour;

/// <summary>
/// Шина сообщений, которая не знает о транспортном уровне.
/// </summary>
internal abstract class AbstractBus<TConfiguration> : IBus
    where TConfiguration : BusConfigurationBase
{
    /// <summary>
    /// Отслеживает работу компонентов шины сообщений.
    /// </summary>
    private readonly IBusComponentTracker componentTracker = new BusComponentTracker();

    /// <summary>
    /// Подготовка, запуск и остановка не выполняются одновременно.
    /// </summary>
    private readonly SemaphoreSlim semaphore = new(1, 1);

    /// <summary>
    /// Отменяется с началом остановки: прерывает ещё не завершённые подготовку и запуск.
    /// </summary>
    private readonly CancellationTokenSource busWorkingTokenSource = new();

    private readonly TaskCompletionSource<int> busStartedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private bool receiversPrepared;

    private bool producersPrepared;

    /// <summary>
    /// Журнал шины сообщений.
    /// </summary>
    protected abstract ILogger Logger { get; }

    protected abstract IReadOnlyCollection<IReceiverConfiguration> ReceiverConfigurations { get; }

    protected abstract IReadOnlyCollection<ISenderConfiguration> SenderConfigurations { get; }

    protected AbstractBus(TConfiguration configuration)
    {
        Configuration = configuration;

        if (Configuration.LifecycleHandler != null)
        {
            Starting += Configuration.LifecycleHandler.OnStarting;
            Started += Configuration.LifecycleHandler.OnStarted;
            Stopping += Configuration.LifecycleHandler.OnStopping;
            Stopped += Configuration.LifecycleHandler.OnStopped;
        }
    }

    /// <summary>
    /// Событие запуска шины сообщений.
    /// </summary>
    public event Action<IBus> Started;

    /// <summary>
    /// Событие начала запуска шины сообщений.
    /// </summary>
    public event Action<IBus> Starting;

    /// <summary>
    /// Событие останова шины сообщений.
    /// </summary>
    public event Action<IBus> Stopped;

    /// <summary>
    /// Событие начала останова шины сообщений.
    /// </summary>
    public event Action<IBus> Stopping;

    /// <summary>
    /// Отслеживает работу компонентов шины.
    /// </summary>
    public IBusComponentTracker ComponentTracker => this.componentTracker;

    /// <summary>
    /// Конфигурация шины.
    /// </summary>
    public TConfiguration Configuration { get; }

    /// <summary>
    /// Конечная точка шины.
    /// </summary>
    public IEndpoint Endpoint => Configuration.Endpoint;

    /// <inheritdoc />
    public abstract string Transport { get; }

    /// <inheritdoc />
    public int ActiveRequestsCount => ComponentTracker.AllOf<IReceiver>().Sum(c => c.ActiveRequestsCount);

    /// <summary>
    /// Готова ли шина к работе.
    /// </summary>
    public bool IsReady => State == BusState.Working;

    /// <inheritdoc />
    public BusState State { get; protected set; } = BusState.NotInitialized;

    /// <inheritdoc />
    public bool IsHealthy => State == BusState.Working && ComponentTracker.CheckHealth();

    /// <summary>
    /// Обработчик меток сообщений.
    /// </summary>
    public IMessageLabelHandler MessageLabelHandler => Configuration.MessageLabelHandler;

    /// <summary>
    /// Преобразователь сообщений.
    /// </summary>
    public IPayloadConverter PayloadConverter => Configuration.Serializer;

    /// <summary>
    /// Получатели сообщений.
    /// </summary>
    public IEnumerable<IReceiver> Receivers => this.componentTracker.AllOf<IReceiver>();

    /// <summary>
    /// Отправители сообщений.
    /// </summary>
    public IEnumerable<ISender> Senders => this.componentTracker.AllOf<ISender>();

    /// <summary>
    /// Проверяет возможность обрабатывать сообщения с указанной меткой.
    /// </summary>
    /// <param name="label">Метка сообщения.</param>
    /// <returns><c>true</c> - если шина сообщения умеет обрабатывать указанную метку сообщений.</returns>
    public bool CanHandle(string label)
    {
        return ReceiverConfigurations.Any(cc => cc.Label.Equals(MessageLabel.From(label)));
    }

    /// <summary>
    /// Проверяет возможность построить маршрут для сообщения с указанной меткой.
    /// </summary>
    /// <param name="label">Метка сообщения.</param>
    /// <returns><c>true</c> - если шина сообщения умеет строить маршрут для указанной метки сообщений.</returns>
    public bool CanRoute(string label)
    {
        var messageLabel = MessageLabel.From(label);
        return SenderConfigurations.Any(pc => pc.Label.Equals(messageLabel) || pc.Key != null && pc.Key.Equals(messageLabel.Name) || pc.Label.IsAny);
    }

    /// <summary>
    /// Получает label на основе алиаса
    /// </summary>
    /// <param name="alias">алиас сообщения</param>
    /// <returns>label сообщения</returns>
    public MessageLabel Resolve(string alias)
    {
        var label = MessageLabel.From(alias);

        return SenderConfigurations.FirstOrDefault(pc => pc.Label.Equals(label) || (pc.Key != null && pc.Key.Equals(label.Name)))?.Label;
    }

    /// <summary>
    /// Уничтожает шину сообщений.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (State != BusState.Stopped)
        {
            await Stop();
        }
    }

    /// <summary>
    /// Посылает сообщение.
    /// Для отправки необходимо указать метку сообщения, на основе которой строится маршрут сообщения.
    /// </summary>
    /// <param name="label">Метка посылаемого сообщения.</param>
    /// <param name="payload">Тело сообщения.</param>
    /// <param name="options">Параметры отправки сообщения.</param>
    /// <typeparam name="T">Тип отправляемого сообщения.</typeparam>
    /// <returns>Задача отправки сообщения.</returns>
    public async Task<EmitResult> EmitAsync<T>(string label, T payload, PublishingOptions options = null) where T : class
    {
        var messageLabel = MessageLabel.From(label);
        DiagnosticProps.EnsureCreated();
        EnsureCanSendUsing(messageLabel);
        await EnsureIsReady();

        return await GetSenderFor(messageLabel)
            .Send(messageLabel, payload, options ?? new PublishingOptions());
    }

    /// <summary>
    /// Асинхронно посылает сообщение в формате запрос-ответ.
    /// Для отправки необходимо указать метку сообщения, на основе которой строится маршрут сообщения.
    /// </summary>
    /// <param name="label">Метка посылаемого сообщения.</param>
    /// <param name="request">Тело запроса.</param>
    /// <param name="options">Параметры отправки запроса.</param>
    /// <typeparam name="TRequest">Тип запроса.</typeparam>
    /// <typeparam name="TResponse">Тип ответа.</typeparam>
    /// <returns>Задача отправки сообщения.</returns>
    public async Task<RequestResult<TResponse>> RequestAsync<TRequest, TResponse>(string label, TRequest request, RequestOptions options = null) where TResponse : class where TRequest : class
    {
        var messageLabel = MessageLabel.From(label);
        DiagnosticProps.EnsureCreated();
        EnsureCanSendUsing(messageLabel);
        await EnsureIsReady();

        return await GetSenderFor(messageLabel)
            .Request<TRequest, TResponse>(messageLabel, request, options ?? new RequestOptions());
    }

    /// <summary>
    /// Предварительное конфигурирование клиента шины без его запуска.
    /// </summary>
    public async Task Prepare(CancellationToken cancellationToken = default)
    {
        if (State == BusState.Stopped)
        {
            Logger.LogWarning("Bus is already stopped and cannot be restarted");
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(this.busWorkingTokenSource.Token, cancellationToken);
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            await Configure(cts.Token);
            return 0;
        }, () => State == BusState.NotInitialized, logger: Logger, cancellationToken: cts.Token);
    }

    /// <summary>
    /// Запускает шину сообщений.
    /// </summary>
    public async Task Start(CancellationToken cancellationToken = default)
    {
        if (State == BusState.Working)
        {
            Logger.LogWarning("Bus is already started");
            return;
        }

        if (State == BusState.Stopped)
        {
            Logger.LogWarning("Bus is already stopped and cannot be restarted");
            return;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(this.busWorkingTokenSource.Token, cancellationToken);
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            Logger.LogTrace("{Endpoint}: Starting...", Endpoint);
            await Configure(cts.Token);
            cts.Token.ThrowIfCancellationRequested();
            await StartComponents(cts.Token);
            return 0;
        }, () => State is not (BusState.Working or BusState.Stopped), logger: Logger, cancellationToken: cts.Token);
    }

    /// <summary>
    /// Останавливает шину сообщений: сначала получение, затем, дождавшись обрабатываемых сообщений, отправку.
    /// </summary>
    public async Task Stop(CancellationToken cancellationToken = default)
    {
        if (State is BusState.Stopped)
        {
            Logger.LogWarning("BusState {State} is already stopped", State);
            return;
        }

        await StopReceiving(cancellationToken);
        await WaitForRequestsCompletion(cancellationToken);
        await StopProducing(cancellationToken);
        await TryStop();
    }

    /// <inheritdoc />
    public async Task StopReceiving(CancellationToken token = default)
    {
        this.busWorkingTokenSource.Cancel();

        await this.semaphore.ExecuteWithCondition(async () =>
        {
            try
            {
                TryEmitStoppingEvent();
                Logger.LogTrace("{Endpoint}: stopping bus receiving components", Endpoint);
                await ComponentTracker.StopAllReceivers(token);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error occured on stopping receiving");
                throw;
            }
            finally
            {
                this.receiversPrepared = false;
            }

            await TryStop();

            return 0;
        }, () => this.receiversPrepared && State is BusState.Configured or BusState.Working, logger: Logger, cancellationToken: token);
    }

    /// <inheritdoc />
    public async Task WaitForRequestsCompletion(CancellationToken token = default)
    {
        while (ActiveRequestsCount > 0)
        {
            Logger.LogInformation("Currently there are {ActiveRequestsCount} active requests. Wait until completion", ActiveRequestsCount);
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }
    }

    /// <inheritdoc />
    public async Task StopProducing(CancellationToken token = default)
    {
        await this.semaphore.ExecuteWithCondition(async () =>
        {
            try
            {
                TryEmitStoppingEvent();

                // Слушатели ответов на запросы останавливаются вместе со своими продюсерами.
                Logger.LogTrace("{Endpoint}: stopping bus sending components", Endpoint);
                await ComponentTracker.StopAllSenders(token);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Error occured on stopping producers");
                throw;
            }
            finally
            {
                this.producersPrepared = false;
            }

            await TryStop();

            return 0;
        }, () => this.producersPrepared && State is BusState.Configured or BusState.Working, logger: Logger, cancellationToken: token);
    }

    /// <summary>
    /// Регистрирует в <see cref="ComponentTracker"/> отправителей и получателей из конфигурации.
    /// </summary>
    protected abstract void RegisterComponents();

    /// <summary>
    /// Проверяет компоненты после их подготовки; ошибка конфигурации прерывает запуск.
    /// </summary>
    protected virtual void ValidateComponents()
    {
    }

    /// <summary>
    /// Освобождает ресурсы транспорта после остановки всех компонентов.
    /// </summary>
    protected virtual Task ReleaseResources()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Вычисляет отправителя для указанной метки сообщений.
    /// </summary>
    /// <param name="label">Метка отправляемого сообщения.</param>
    /// <returns>Отправитель сообщения с указанной меткой.</returns>
    /// <exception cref="BusConfigurationException">Генерируется исключение, если нет отправителя для указанной метки.</exception>
    protected ISender GetSenderFor(MessageLabel label)
    {
        var sender = Senders.FirstOrDefault(s => s.CanRoute(label)) ?? Senders.FirstOrDefault(s => s.CanRoute(MessageLabel.Any));

        if (sender == null)
        {
            throw new BusConfigurationException("No sender is configured to send [{0}].".FormatEx(label));
        }

        return sender;
    }

    /// <summary>
    /// Генерирует событие о запуске шины сообщений.
    /// </summary>
    protected void OnStarted()
    {
        Logger.LogInformation("Started [{BusType}] with endpoint [{Endpoint}]", GetType().Name, Endpoint);

        Started?.Invoke(this);
    }

    /// <summary>
    /// Генерирует событие о начале запуска шины сообщений.
    /// </summary>
    protected void OnStarting()
    {
        Logger.LogInformation("Starting [{BusType}] with endpoint [{Endpoint}]", GetType().Name, Endpoint);

        Starting?.Invoke(this);
    }

    /// <summary>
    /// Генерирует событие об остановке шины сообщений.
    /// </summary>
    protected void OnStopped()
    {
        Logger.LogInformation("Stopped [{BusType}] with endpoint [{Endpoint}]", GetType().Name, Endpoint);

        Stopped?.Invoke(this);
    }

    /// <summary>
    /// Генерирует событие о начале остановки шины сообщений.
    /// </summary>
    protected void OnStopping()
    {
        Logger.LogInformation("Stopping [{BusType}] with endpoint [{Endpoint}]", GetType().Name, Endpoint);

        Stopping?.Invoke(this);
    }

    /// <summary>
    /// Проверяет возможность отправить сообщение с указанной меткой.
    /// </summary>
    /// <param name="label">Метка отправляемого сообщения.</param>
    /// <exception cref="InvalidOperationException">Генерируется если нельзя отправлять указанные метки сообщения.</exception>
    private async Task StartComponents(CancellationToken token)
    {
        try
        {
            OnStarting();

            Logger.LogTrace("{Endpoint}: starting senders", Endpoint);
            await ComponentTracker.StartAllSenders(token);
            token.ThrowIfCancellationRequested();

            Logger.LogTrace("{Endpoint}: starting receivers", Endpoint);
            await ComponentTracker.StartAllReceivers(token);
            token.ThrowIfCancellationRequested();

            State = BusState.Working;
            Logger.LogTrace("{Endpoint}: marking as ready", Endpoint);
            this.busStartedTcs.TrySetResult(1);
            OnStarted();
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Error on starting bus: {EndpointAddress}", Endpoint.Address);
            throw;
        }
    }

    private async Task Configure(CancellationToken token)
    {
        if (State != BusState.NotInitialized)
        {
            return;
        }

        token.ThrowIfCancellationRequested();

        try
        {
            var name = GetType().Name;
            Logger.LogTrace("{Endpoint}: configuring", Endpoint);

            RegisterComponents();

            Logger.LogTrace("{Endpoint}: preparing components", Endpoint);
            await ComponentTracker.PrepareAll(token);
            ValidateComponents();

            this.producersPrepared = true;
            this.receiversPrepared = true;

            token.ThrowIfCancellationRequested();

            Logger.LogInformation("Configuration of [{Name}] completed successfully", name);
            State = BusState.Configured;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Failed to configure bus: {EndpointAddress}", Endpoint.Address);
            throw;
        }
    }

    private void TryEmitStoppingEvent()
    {
        if (this.producersPrepared ^ this.receiversPrepared)
        {
            OnStopping();
        }
    }

    private async Task TryStop()
    {
        if (this.producersPrepared || this.receiversPrepared || State is not (BusState.Working or BusState.Configured))
        {
            return;
        }

        try
        {
            Logger.LogTrace("{Endpoint}: marking as not ready", Endpoint);
            this.busStartedTcs.TrySetCanceled();
            await ComponentTracker.UnregisterAll();
            await ReleaseResources();
        }
        finally
        {
            State = BusState.Stopped;
            OnStopped();
        }
    }

    private static void EnsureCanSendUsing(MessageLabel label)
    {
        if (label.IsAny)
        {
            throw new InvalidOperationException("Can't send using Any label.");
        }

        if (label.IsEmpty)
        {
            throw new InvalidOperationException("Can't send using Empty label.");
        }
    }

    /// <summary>
    /// Проверяет, что шина сообщений готова к работе.
    /// </summary>
    /// <exception cref="BusNotReadyException">Если шина не готова к работе, генерируется исключение.</exception>
    private async Task EnsureIsReady()
    {
        if (this.busStartedTcs.Task.IsCompleted && !IsReady)
        {
            throw new BusNotReadyException();
        }

        var timeoutTask = Task.Delay(TimeSpan.FromSeconds(5));
        var completedTask = await Task.WhenAny(this.busStartedTcs.Task, timeoutTask);

        if (completedTask == timeoutTask)
        {
            throw new BusNotReadyException();
        }
    }
}
