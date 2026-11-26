using System.Threading.Tasks;

namespace Contour.Receiving.Consumers;

/// <summary>
/// The ConsumerOf interface.
/// </summary>
/// <typeparam name="T">
/// </typeparam>
public interface IConsumerOf<T> : IConsumer<T>
    where T : class
{
    Task HandleAsync(IConsumingContext<T> context);
}