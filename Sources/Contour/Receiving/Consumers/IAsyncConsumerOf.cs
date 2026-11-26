using System;
using System.Threading.Tasks;

namespace Contour.Receiving.Consumers;

/// <summary>
/// Асинхронный интерфейс, сделано наследование, что бы сохранить обратную совместимость, а так же дать возможность не править DI
/// </summary>
/// <typeparam name="T"></typeparam>
[Obsolete("Use IConsumerOf<T> interface instead")]
public interface IAsyncConsumerOf<T> : IConsumer<T>
    where T : class
{
    Task HandleAsync(IConsumingContext<T> context);
}