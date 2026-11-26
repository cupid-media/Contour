using System.Threading.Tasks;

namespace Contour.Sending;

/// <summary>
/// Отправитель сообщений.
/// </summary>
public interface ISender : IBusComponent
{
    /// <summary>
    /// Проверяет возможность создать маршрут для указанной метки сообщения.
    /// </summary>
    /// <param name="label">Метка сообщения, для которой необходимо построить маршрут.</param>
    /// <returns><c>true</c> - если для указанной метки можно построить маршрут.</returns>
    bool CanRoute(MessageLabel label);

    /// <summary>
    /// Отправляет сообщение в формате запрос-ответ.
    /// </summary>
    /// <param name="label">Метка отправляемого запроса.</param>
    /// <param name="payload">Сообщение запроса.</param>
    /// <param name="options">Параметры запроса.</param>
    /// <returns>Задача выполнения запроса.</returns>
    Task<RequestResult<TResponse>> Request<TRequest, TResponse>(MessageLabel label, TRequest payload, RequestOptions options) where TRequest : class where TResponse : class;

    /// <summary>
    /// Отправляет одностороннее сообщение.
    /// </summary>
    /// <param name="label">Метка отправляемого сообщения.</param>
    /// <param name="payload">Тело сообщения.</param>
    /// <param name="options">Заголовки сообщения.</param>
    /// <returns>Задача выполнения отправки сообщения.</returns>
    Task<EmitResult> Send<TRequest>(MessageLabel label, TRequest payload, PublishingOptions options) where TRequest : class;
}