using System;

namespace Contour.Sending.Exceptions;

public class FailedToSendMessageException : Exception
{
    internal FailedToSendMessageException(string message) : base(message) { }

    internal FailedToSendMessageException(string message, Exception innerException) : base(message, innerException) { }
}
