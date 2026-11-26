using System.Collections.Generic;

namespace Contour;

public sealed record Message<TModel>(MessageLabel Label, TModel Payload, IDictionary<string, object> Headers)
    : IMessage where TModel : class
{
    object IMessage.Payload => Payload;

    /// <summary>
    /// Ключ сообщения из <see cref="Sending.PublishingOptions.Key"/>; используется транспортом Kafka.
    /// </summary>
    public string Key { get; init; }
}
