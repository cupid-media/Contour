namespace Contour.Sending;

public record RequestResult<T> : EmitResult
{
    public T Response { get; init; }
}
