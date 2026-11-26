using System;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;

namespace Contour.Topology;

/// <summary>
/// Построитель топологии.
/// </summary>
public interface ITopologyBuilder : IDisposable
{
    /// <summary>
    /// Создает временную конечную точку для получения сообщений.
    /// </summary>
    /// <returns>Конечная точка подписки для получения сообщений.</returns>
    Task<ISubscriptionEndpoint> BuildTempReplyEndpoint(CancellationToken cancellationToken);

    /// <summary>
    /// Создает временную конечную точку для получения сообщений.
    /// </summary>
    /// <param name="endpoint">Конечная точка шины сообщений для который создается подписка.</param>
    /// <param name="label">Метка сообщений, на которые ожидается получение ответа.</param>
    /// <returns>
    /// Конечная точка подписки для получения сообщений.
    /// </returns>
    Task<ISubscriptionEndpoint> BuildTempReplyEndpoint(IEndpoint endpoint, MessageLabel label, CancellationToken cancellationToken);
}