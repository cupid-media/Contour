using Contour.Serialization;

namespace Contour.Configuration;

public abstract record BusConfigurationBase
{
    public IEndpoint Endpoint { get; set; }

    public IBusLifecycleHandler LifecycleHandler { get; set; }

    public IMessageLabelHandler MessageLabelHandler { get; set; }

    public IPayloadConverter Serializer { get; set; }
}