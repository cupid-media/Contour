using System.Collections.Generic;
using System.Linq;

namespace Contour.Sending.Exceptions;

public class NoAvailableProducersFoundException : FailedToSendMessageException
{
    internal NoAvailableProducersFoundException(IEnumerable<IProducer> producers)
        : base($"Unable to take the next producer because no available producers left. Total producers: {producers.Count()}")
    {
    }
}
