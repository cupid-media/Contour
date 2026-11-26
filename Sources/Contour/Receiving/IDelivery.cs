using System.Threading;
using System.Threading.Tasks;

namespace Contour.Receiving;

/// <summary>
/// Доставленное сообщение.
/// </summary>
public interface IDelivery
{
    /// <summary>
    /// Метка доставленных сообщений.
    /// </summary>
    MessageLabel Label { get; }

    /// <summary>
    /// Верно, если можно ответить на это сообщение.
    /// </summary>
    bool CanReply { get; }

    /// <summary>
    /// Подтверждает доставку сообщения.
    /// </summary>
    Task AcceptAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Пересылает сообщение, устанавливая указанную метку.
    /// </summary>
    /// <param name="label">Новая метка, с которой пересылается сообщение.</param>
    /// <param name="payload">Новое содержимое сообщения.</param>
    /// <returns>Задача пересылки сообщения.</returns>
    Task ForwardAsync(MessageLabel label, object payload);

    /// <summary>
    /// Помечает сообщение как необработанное.
    /// </summary>
    /// <param name="requeue">
    /// Сообщение требуется вернуть во входящую очередь для повторной обработки.
    /// </param>
    /// <param name="cancellationToken"></param>
    Task RejectAsync(bool requeue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Отсылает ответное сообщение.
    /// </summary>
    /// <param name="message">Ответное сообщение.</param>
    /// <param name="cancellationToken"></param>
    Task ReplyWithAsync<TReply>(Message<TReply> message, CancellationToken cancellationToken = default) where TReply : class;

    /// <summary>
    /// Конвертирует полученную информацию в сообщение указанного типа.
    /// </summary>
    /// <returns>Сообщение указанного типа.</returns>
    Message<TResponse> UnpackAs<TResponse>() where TResponse : class;
}
