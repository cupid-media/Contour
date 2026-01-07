using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Contour.Sending;

namespace Contour.Tracing
{
    /// <summary>
    /// Provides W3C Trace Context header propagation for distributed tracing in Contour
    /// </summary>
    public static class W3CTraceContextProvider
    {
        /// <summary>
        /// Extracts W3C trace context (traceparent and tracestate) from message headers.
        /// </summary>
        public static bool TryExtractTraceContext(IMessage message, out string traceParent, out string traceState)
        {
            traceParent = null;
            traceState = null;

            if (!TryGetTraceParent(message, out traceParent))
                return false;

            if (message.Headers.TryGetValue(Headers.TraceState, out var traceStateObj))
            {
                traceState = traceStateObj as string ?? Encoding.UTF8.GetString((byte[])traceStateObj);
            }

            return true;
        }

        /// <summary>
        /// Copies trace context from the source message into outgoing headers.
        /// Creates a new span id when reusing an existing traceparent.
        /// </summary>
        public static void InjectTraceContext(IDictionary<string, object> headers, IMessage sourceMessage)
        {
            if (headers == null)
                return;

            try
            {
                if (sourceMessage?.Headers == null)
                    return;

                if (TryGetTraceParent(sourceMessage, out var traceParent))
                {
                    if (TryParseW3CTraceParent(traceParent, out var traceId, out var _, out var traceFlags))
                    {
                        var newSpanId = GenerateSpanId();
                        var newTraceParent = $"00-{traceId}-{newSpanId}-{traceFlags:x2}";
                        headers[Headers.TraceParent] = newTraceParent;
                    }
                    else
                    {
                        headers[Headers.TraceParent] = traceParent;
                    }
                }

                if (sourceMessage.Headers.TryGetValue(Headers.TraceState, out var traceStateObj))
                {
                    var traceState = traceStateObj as string ?? Encoding.UTF8.GetString((byte[])traceStateObj);
                    headers[Headers.TraceState] = traceState;
                }
            }
            catch (Exception)
            {
                // Swallow to avoid impacting message flow
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

            if (parts[1].Length != 32 || !IsValidHexString(parts[1]))
                return false;
            traceId = parts[1];

            if (parts[2].Length != 16 || !IsValidHexString(parts[2]))
                return false;
            spanId = parts[2];

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
