using System.Collections.Generic;

namespace Contour;

public interface IMessage
{
    MessageLabel Label { get; }

    IDictionary<string, object> Headers { get; }

    object Payload { get; }
}
