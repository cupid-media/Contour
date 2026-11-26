using System.Text.Json;

namespace Contour.Serialization;

public class JsonPayloadConverter : IPayloadConverter
{
    public string ContentType => "application/json";

    public byte[] FromObject(object payload)
    {
        return JsonSerializer.SerializeToUtf8Bytes(payload);
    }

    public T ToObject<T>(byte[] payload)
    {
        return JsonSerializer.Deserialize<T>(payload);
    }
}
