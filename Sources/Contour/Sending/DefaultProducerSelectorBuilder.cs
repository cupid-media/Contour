using System.Collections.Generic;

namespace Contour.Sending;

internal class DefaultProducerSelectorBuilder : IProducerSelectorBuilder
{
    public IProducerSelector Build(IEnumerable<IProducer> items)
    {
        return new RoundRobinGoodConditionProducerSelector(items);
    }
}