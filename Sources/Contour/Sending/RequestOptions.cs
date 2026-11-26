using System;

namespace Contour.Sending;

/// <summary>
/// The request options.
/// </summary>
public class RequestOptions : PublishingOptions
{
    /// <summary>
    /// Gets or sets the timeout.
    /// </summary>
    public TimeSpan? Timeout { get; set; }
}