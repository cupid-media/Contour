namespace Contour.Serialization;

/// <summary>
///   Конвертер сообщений.
/// </summary>
public interface IPayloadConverter
{
    /// <summary>
    /// Gets the content type.
    /// </summary>
    string ContentType { get; }

    /// <summary>
    /// The from object.
    /// </summary>
    /// <param name="payload">
    /// The payload.
    /// </param>
    /// <returns>
    /// The <see cref="byte"/>.
    /// </returns>
    byte[] FromObject(object payload);

    /// <summary>
    /// The to object.
    /// </summary>
    /// <param name="payload">
    /// The payload.
    /// </param>
    /// <returns>
    /// The <see cref="object"/>.
    /// </returns>
    T ToObject<T>(byte[] payload);
}