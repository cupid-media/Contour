using System;

namespace Contour.Sending;

/// <summary>
/// Конфигурация отправителя.
/// </summary>
public interface ISenderConfiguration
{
    /// <summary>
    /// Псевдоним для метки отправляемых сообщений.
    /// </summary>
    string Key { get; }

    /// <summary>
    /// Метка отправляемых сообщений.
    /// </summary>
    MessageLabel Label { get; }

    IIncomingMessageHeaderStorage IncomingMessageHeaderStorage { get; }

    bool Persistently { get; }

    bool Delayed { get; }

    TimeSpan? Ttl { get; }

    TimeSpan? RequestTimeout { get; }

    /// <summary>
    /// Определяет необходимо ли использовать обратный вызов для обработки ответных сообщений.
    /// </summary>
    bool RequiresCallback { get; }
}