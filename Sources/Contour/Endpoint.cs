namespace Contour;

/// <summary>
/// Конечная точка шины сообщений.
/// Универсальный адаптер для передачи и получения сообщений из шины.
/// </summary>
internal record Endpoint(string Address) : IEndpoint
{
    /// <summary>
    /// Формирует строковое представление конечной точки.
    /// </summary>
    /// <returns>Строковое представление конечной точки.</returns>
    public override string ToString()
    {
        return Address;
    }
}