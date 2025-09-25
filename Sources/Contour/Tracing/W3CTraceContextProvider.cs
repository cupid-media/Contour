using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Contour.Sending;
using System.Diagnostics;

namespace Contour.Tracing
{
    /// <summary>
    /// Provides W3C Trace Context header propagation for distributed tracing in Contour
    /// </summary>
    public static class W3CTraceContextProvider
    {
        /// <summary>
        /// Extracts parent ActivityContext from message headers for the consuming application
        /// </summary>
        /// <param name="message">Incoming message with potential trace context</param>
        /// <param name="traceParent">Extracted traceparent header value</param>
        /// <param name="traceState">Extracted tracestate header value</param>
        /// <returns>True if valid trace context was extracted</returns>
        public static bool TryExtractTraceContext(IMessage message, out string traceParent, out string traceState)
        {
            traceParent = null;
            traceState = null;

            if (!TryGetTraceParent(message, out traceParent))
                return false;

            // Also extract tracestate if available
            if (message.Headers.TryGetValue(Headers.TraceState, out var traceStateObj))
            {
                traceState = traceStateObj as string ?? Encoding.UTF8.GetString((byte[])traceStateObj);
            }

            return true;
        }

        /// <summary>
        /// Copies trace context from current activity or source message to outgoing message headers, 
        /// generating a new span ID for the outgoing message
        /// </summary>
        /// <param name="headers">Message headers to inject trace context into</param>
        /// <param name="sourceMessage">Source message to copy trace context from if no current activity</param>
        public static void InjectTraceContext(IDictionary<string, object> headers, IMessage? sourceMessage)
        {
            if (headers == null)
                return;

            try
            {
                var currentActivity = Activity.Current;
                
                // Prefer current activity context if available
                if (currentActivity != null && !string.IsNullOrEmpty(currentActivity.Id))
                {
                    // Use current activity's trace context (already in W3C format)
                    headers[Headers.TraceParent] = currentActivity.Id;
                    
                    if (!string.IsNullOrEmpty(currentActivity.TraceStateString))
                    {
                        headers[Headers.TraceState] = currentActivity.TraceStateString;
                    }
                    return;
                }

                // Fallback to copying from source message if no current activity
                if (sourceMessage?.Headers == null)
                    return;

                // Copy traceparent with new span ID from source message
                if (TryGetTraceParent(sourceMessage, out var traceParent))
                {
                    // Parse and create new span for the outgoing message
                    if (TryParseW3CTraceParent(traceParent, out var traceId, out var _, out var traceFlags))
                    {
                        var newSpanId = GenerateSpanId();
                        var newTraceParent = $"00-{traceId}-{newSpanId}-{traceFlags:x2}";
                        headers[Headers.TraceParent] = newTraceParent;
                    }
                    else
                    {
                        // If parsing fails, copy as-is
                        headers[Headers.TraceParent] = traceParent;
                    }
                }

                // Copy tracestate from source message
                if (sourceMessage.Headers.TryGetValue(Headers.TraceState, out var traceStateObj))
                {
                    var traceState = traceStateObj as string ?? Encoding.UTF8.GetString((byte[])traceStateObj);
                    headers[Headers.TraceState] = traceState;
                }
            }
            catch (Exception)
            {
                // Don't break message sending if trace context copying fails
            }
        }

        private static bool TryGetTraceParent(IMessage message, out string traceParent)
        {
            traceParent = null;
    
            if (message?.Headers == null || !message.Headers.TryGetValue(Headers.TraceParent, out var traceParentObj))
                return false;

            switch (traceParentObj)
            {
                case string str:
                    traceParent = str;
                    break;
                case byte[] bytes:
                    traceParent = Encoding.UTF8.GetString(bytes);
                    break;
                default:
                    traceParent = null;
                    break;
            }

            return !string.IsNullOrWhiteSpace(traceParent);
        }

        private static bool TryParseW3CTraceParent(string traceParent, out string traceId, out string spanId, out byte traceFlags)
        {
            traceId = null;
            spanId = null;
            traceFlags = 0;

            if (string.IsNullOrEmpty(traceParent))
                return false;

            var parts = traceParent.Split('-');
            if (parts.Length != 4 || parts[0] != "00")
                return false;

            // Validate trace ID (32 hex chars)
            if (parts[1].Length != 32 || !IsValidHexString(parts[1]))
                return false;
            traceId = parts[1];

            // Validate span ID (16 hex chars)  
            if (parts[2].Length != 16 || !IsValidHexString(parts[2]))
                return false;
            spanId = parts[2];

            // Validate trace flags (2 hex chars)
            if (parts[3].Length != 2 || !byte.TryParse(parts[3], NumberStyles.HexNumber, null, out traceFlags))
                return false;

            return true;
        }

        private static bool IsValidHexString(string hex)
        {
            foreach (var c in hex)
            {
                if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
                    return false;
            }
            return true;
        }

        private static readonly Random Random = new Random();

        private static string GenerateSpanId()
        {
            var bytes = new byte[8];
            lock (Random)
            {
                Random.NextBytes(bytes);
            }
            return BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant();
        }
    }
}
