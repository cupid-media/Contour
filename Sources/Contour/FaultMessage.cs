using System;

namespace Contour;

/// <summary>
/// The fault message.
/// </summary>
public class FaultMessage
{
    public FaultMessage(Message<object> message, DateTime received, string contentType, Exception exception = null)
    {
        Received = received;
        Label = message.Label.Name;
        Payload = message.Payload;
        ContentType = contentType;
        Exception = exception != null ? new FaultException(exception) : null;
    }

    public FaultMessage(){}

    /// <summary>
    /// Gets the content type.
    /// </summary>
    public string ContentType { get; set; }

    /// <summary>
    /// Gets the exception.
    /// </summary>
    public FaultException Exception { get; set; }

    /// <summary>
    /// Gets the label.
    /// </summary>
    public string Label { get; set; }

    /// <summary>
    /// Gets the payload.
    /// </summary>
    public object Payload { get; set; }

    /// <summary>
    /// Gets the received.
    /// </summary>
    public DateTime Received { get; set; }
}