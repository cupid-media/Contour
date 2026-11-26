namespace Contour.Receiving.Consumers;

/// <summary>
/// The Generic Consumer interface. Нужен чтобы не потерять тип при резолве зависимостей.
/// </summary>
public interface IConsumer<T> : IConsumer
{
}

/// <summary>
/// The Consumer interface.
/// </summary>
public interface IConsumer
{
}