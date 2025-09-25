using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Contour.Tracing;

public static class MessageExtensions
{
    /// <summary>
    /// Starts a new activity with trace context from the message as parent
    /// </summary>
    /// <param name="message">The message to extract trace context from</param>
    /// <param name="name">Activity name. Defaults to caller method name.</param>
    /// <param name="kind">Activity kind (default: Consumer)</param>
    /// <returns>New activity with proper parent context, or null if activity source is not enabled</returns>
    internal static Activity? StartActivityWithMessageContext(
        this IMessage message,
        [CallerMemberName] string name = "",
        ActivityKind kind = ActivityKind.Consumer)
    {
        if (!message.TryGetActivityContext(out var parentContext))
        {
            // Create activity without parent context if extraction fails
            return ContourActivitySource.Source.StartActivity(name, kind);
        }

        return ContourActivitySource.Source.StartActivity(name, kind, parentContext);
    }

    /// <summary>
    /// Starts a new activity with trace context from the message as parent, with tags
    /// </summary>
    /// <param name="message">The message to extract trace context from</param>
    /// <param name="name">Activity name. Defaults to caller method name</param>
    /// <param name="kind">Activity kind</param>
    /// <param name="tags">Activity tags</param>
    /// <returns>New activity with proper parent context and tags</returns>
    internal static Activity? StartActivityWithMessageContext<T>(
        this IMessage message,
        IEnumerable<KeyValuePair<string, object>> tags,
        [CallerMemberName] string name = "",
        ActivityKind kind = ActivityKind.Consumer) where T : class
    {
        Activity activity;

        if (!message.TryGetActivityContext(out var parentContext))
        {
            activity = ContourActivitySource.Source.StartActivity(name, kind, default(ActivityContext), tags);
        }
        else
        {
            activity = ContourActivitySource.Source.StartActivity(name, kind, parentContext, tags);
        }

        return activity;
    }
    
    /// <summary>
    /// Tries to extract W3C trace context from message headers to create parent ActivityContext
    /// </summary>
    /// <param name="parentContext">The extracted parent ActivityContext if successful</param>
    /// <returns>True if valid trace context was extracted and parsed successfully</returns>
    private static bool TryGetActivityContext(this IMessage message, out ActivityContext parentContext)
    {
        parentContext = default;
        
        if (!W3CTraceContextProvider.TryExtractTraceContext(message, out var traceParent, out var traceState))
            return false;

        try
        {
            // Parse the W3C traceparent format: 00-{traceId}-{spanId}-{flags}
            var parts = traceParent.Split('-');
            if (parts.Length != 4 || parts[0] != "00")
                return false;

            var traceId = ActivityTraceId.CreateFromString(parts[1].AsSpan());
            var spanId = ActivitySpanId.CreateFromString(parts[2].AsSpan());
            var traceFlags = (ActivityTraceFlags)byte.Parse(parts[3], System.Globalization.NumberStyles.HexNumber);

            parentContext = new ActivityContext(traceId, spanId, traceFlags, traceState);
            return true;
        }
        catch
        {
            return false;
        }
    }
}