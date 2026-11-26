using System;

namespace Contour.Sending.Exceptions;

/// <summary>
/// An exception is raised if the bus was unable to handle a message in a fault tolerant manner, ex. publish attempts exhausted.
/// </summary>
public class FailoverException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FailoverException"/> class.
    /// </summary>
    internal FailoverException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FailoverException"/> class.
    /// </summary>
    /// <param name="message">
    /// The message.
    /// </param>
    internal FailoverException(string message) : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FailoverException"/> class.
    /// </summary>
    /// <param name="message">
    /// The message.
    /// </param>
    /// <param name="inner">
    /// The inner.
    /// </param>
    internal FailoverException(string message, Exception inner) : base(message, inner)
    {
    }

    /// <summary>
    /// Gets or sets the attempts taken to complete an operation
    /// </summary>
    public int Attempts { get; internal set; }
}
