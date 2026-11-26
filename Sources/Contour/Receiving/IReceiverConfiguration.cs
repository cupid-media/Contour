namespace Contour.Receiving;

/// <summary>
/// Дополнительная конфигурация получателя сообщения.
/// </summary>
public interface IReceiverConfiguration
{
    /// <summary>
    /// Метка получаемого сообщения.
    /// </summary>
    MessageLabel Label { get; }
}