using System.Threading.Tasks;

namespace Contour.Transport.Kafka.Internal;

internal sealed class ConsumingResult
{
    public static readonly ConsumingResult Acknowledged = new(Task.FromResult(true));

    private static readonly ConsumingResult Held = new(Task.FromResult(false));

    private ConsumingResult(Task<bool> acknowledgeTask)
    {
        AcknowledgeTask = acknowledgeTask;
    }

    public Task<bool> AcknowledgeTask { get; }

    public bool IsCompleted => AcknowledgeTask.IsCompleted;

    public bool? IsAcknowledged => AcknowledgeTask.IsCompletedSuccessfully ? AcknowledgeTask.Result : null;

    public static ConsumingResult FromResult(bool acknowledged) => acknowledged ? Acknowledged : Held;

    public static ConsumingResult Awaited(Task<bool> decision) => new(decision);
}
