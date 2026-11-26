using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Sending;

namespace Contour;

/// <summary>
///   Клиент шины сообщений.
///   Используется для управления циклом жизни клиента шины, а также для передачи сообщений.
/// </summary>
public interface IBus : IAsyncDisposable
{
    /// <summary>
    ///   Описание экземпляра шины
    /// </summary>
    IEndpoint Endpoint { get; }

    /// <summary>
    ///   Транспорт, на котором работает шина: <see cref="BusTransports.Rmq"/> или
    ///   <see cref="BusTransports.Kafka"/>. Позволяет вызывающему коду — например метрикам —
    ///   различать шины, не зная их конкретных типов.
    /// </summary>
    string Transport { get; }

    /// <summary>
    /// Количество сообщений, которые в данный момент обрабатываются шиной
    /// </summary>
    int ActiveRequestsCount { get; }

    /// <summary>
    ///   Установлен при полной готовности шины.
    /// </summary>
    bool IsReady { get; }

    /// <summary>
    ///   Шина запущена и все её компоненты работоспособны. В отличие от <see cref="IsReady"/>,
    ///   отражает не только состояние шины, но и живость её отправителей и получателей,
    ///   поэтому подходит для health/liveness-проб.
    /// </summary>
    bool IsHealthy { get; }

    /// <summary>
    /// Состояние шины
    /// </summary>
    BusState State { get; }

    /// <summary>
    ///   Событие окончания запуска шины
    /// </summary>
    event Action<IBus> Started;

    /// <summary>
    ///   Событие начала запуска шины
    /// </summary>
    event Action<IBus> Starting;

    /// <summary>
    ///   Событие окончания остановки шины
    /// </summary>
    event Action<IBus> Stopped;

    /// <summary>
    ///   Событие начала остановки шины
    /// </summary>
    event Action<IBus> Stopping;

    /// <summary>
    /// Конфигурирование и запуск клиента шины.
    /// </summary>
    /// <returns>Задача запускающая шину.</returns>
    Task Start(CancellationToken cancellationToken = default);

    /// <summary>
    /// Предварительное конфигурирование клиента шины без его запуска.
    /// </summary>
    /// <returns>Задача конфигурирующая шину.</returns>
    Task Prepare(CancellationToken cancellationToken = default);

    /// <summary>
    ///   Остановка клиента шины.
    /// </summary>
    Task Stop(CancellationToken cancellationToken = default);

    /// <summary>
    /// Останавливает получение сообщений, но не останавливает отправку. Используется для плавного завершения работы шины, когда необходимо дождаться обработки всех полученных сообщений, но при этом не принимать новые сообщения.
    /// </summary>
    Task StopReceiving(CancellationToken token = default);

    /// <summary>
    /// Дожидается завершения обработки всех полученных сообщений. Используется после вызова <see cref="StopReceiving"/> для плавного завершения работы шины, когда необходимо дождаться обработки всех полученных сообщений, но при этом не принимать новые сообщения.
    /// </summary>
    Task WaitForRequestsCompletion(CancellationToken token = default);

    /// <summary>
    /// Останавливает отправку сообщений. Используется для плавного завершения работы шины.
    /// </summary>
    Task StopProducing(CancellationToken token = default);

    /// <summary>
    /// Проверяет возможность получения сообщения с указанной меткой.
    /// </summary>
    /// <param name="label">Метка сообщения.</param>
    /// <returns>
    /// Если <c>true</c> - тогда шина может обрабатывать сообщения с такой меткой, иначе - <c>false</c>.
    /// </returns>
    bool CanHandle(string label);

    /// <summary>
    /// Проверяет возможность отправки с использованием указанной метки.
    /// </summary>
    /// <param name="label">Метка сообщения.</param>
    /// <returns>
    /// Если <c>true</c> - тогда шина может отправлять сообщения с такой меткой, иначе - <c>false</c>.
    /// </returns>
    bool CanRoute(string label);

    /// <summary>
    /// Отправляет сообщение с указанной меткой.
    /// </summary>
    /// <remarks>Метка сообщения должна быть зарегистрирована.</remarks>
    /// <typeparam name="T"><c>.NET</c> тип отправляемого сообщения.
    /// </typeparam>
    /// <param name="label">Метка отправляемого сообщения.</param>
    /// <param name="payload">Отправляемое сообщение.</param>
    /// <param name="options">Настройки отправителя.</param>
    /// <returns>
    /// Задача ожидания отправки.
    /// </returns>
    Task<EmitResult> EmitAsync<T>(string label, T payload, PublishingOptions options = null) where T : class;

    /// <summary>Выполняет асинхронный запрос данных с указанной меткой.</summary>
    /// <typeparam name="TRequest">Тип данных запроса.</typeparam>
    /// <typeparam name="TResponse">Тип ожидаемого ответа.</typeparam>
    /// <param name="label">Метка отправляемого сообщения.</param>
    /// <param name="request">Отправляемое сообщение.</param>
    /// <param name="options">Настройки запроса.</param>
    /// <returns>Задача получения ответного сообщения.</returns>
    Task<RequestResult<TResponse>> RequestAsync<TRequest, TResponse>(string label, TRequest request, RequestOptions options = null) where TRequest : class where TResponse : class;

    /// <summary>
    /// Получает label на основе алиаса
    /// </summary>
    /// <param name="alias">алиас сообщения</param>
    /// <returns>label сообщения</returns>
    MessageLabel Resolve(string alias);
}
