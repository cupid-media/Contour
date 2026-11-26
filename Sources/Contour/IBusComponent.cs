using System.Threading.Tasks;
using System;
using System.Threading;

namespace Contour;

/// <summary>
///   Компонент клиента шины сообщений, существующий в течение срока жизни самого клиента.
/// </summary>
public interface IBusComponent : IAsyncDisposable
{
    /// <summary>
    /// Подготавливает компонент
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task Prepare(CancellationToken cancellationToken);

    /// <summary>
    ///   Запуск компонента.
    /// </summary>
    Task Start(CancellationToken cancellationToken);

    /// <summary>
    ///   Остановка компонента.
    /// </summary>
    Task Stop(CancellationToken cancellationToken);

    /// <summary>
    /// Проверка работоспособности компонента.
    /// </summary>
    bool IsHealthy { get; }
}