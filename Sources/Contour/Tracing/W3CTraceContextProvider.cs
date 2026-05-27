using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
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

            if (!TryGetTraceContext(message?.Headers, out traceParent, out traceState))
                return false;

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
                if (TryInjectCurrentActivity(headers))
                    return;

                if (!TryGetTraceContext(sourceMessage?.Headers, out var traceParent, out var traceState) &&
                    !TryGetTraceContext(headers, out traceParent, out traceState))
                {
                    ClearTraceContext(headers);
                    return;
                }

                TryParseW3CTraceParent(traceParent, out var traceId, out var _, out var traceFlags);

                headers[Headers.TraceParent] = $"00-{traceId}-{GenerateSpanId()}-{traceFlags:x2}";
                SetTraceState(headers, traceState);
            }
            catch (Exception)
            {
                // Swallow to avoid impacting message flow
            }
        }

        private static bool TryGetTraceContext(IDictionary<string, object> headers, out string traceParent, out string traceState)
        {
            traceParent = null;
            traceState = null;
    
            if (!TryGetHeaderString(headers, Headers.TraceParent, out traceParent))
                return false;

            if (!TryParseW3CTraceParent(traceParent, out var _, out var _, out var _))
                return false;

            TryGetHeaderString(headers, Headers.TraceState, out traceState);

            return true;
        }

        private static bool TryGetHeaderString(IDictionary<string, object> headers, string key, out string value)
        {
            value = null;

            if (headers == null || !headers.TryGetValue(key, out var headerValue))
                return false;

            switch (headerValue)
            {
                case string str:
                    value = str;
                    break;
                case byte[] bytes:
                    value = Encoding.UTF8.GetString(bytes);
                    break;
                default:
                    return false;
            }

            return !string.IsNullOrWhiteSpace(value);
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

            if (parts[1].Length != 32 || !IsValidHexString(parts[1]) || IsAllZeros(parts[1]))
                return false;
            traceId = parts[1];

            if (parts[2].Length != 16 || !IsValidHexString(parts[2]) || IsAllZeros(parts[2]))
                return false;
            spanId = parts[2];

            if (parts[3].Length != 2 || !byte.TryParse(parts[3], NumberStyles.HexNumber, null, out traceFlags))
                return false;

            return true;
        }

        private static bool TryInjectCurrentActivity(IDictionary<string, object> headers)
        {
            var activity = Activity.Current;
            if (activity == null ||
                IsAllZeros(activity.TraceId.ToString()) ||
                IsAllZeros(activity.SpanId.ToString()))
                return false;

            headers[Headers.TraceParent] = $"00-{activity.TraceId}-{activity.SpanId}-{(byte)activity.ActivityTraceFlags:x2}";
            SetTraceState(headers, activity.TraceStateString);

            return true;
        }

        private static void SetTraceState(IDictionary<string, object> headers, string traceState)
        {
            if (string.IsNullOrWhiteSpace(traceState))
            {
                headers.Remove(Headers.TraceState);
                return;
            }

            headers[Headers.TraceState] = traceState;
        }

        private static void ClearTraceContext(IDictionary<string, object> headers)
        {
            headers.Remove(Headers.TraceParent);
            headers.Remove(Headers.TraceState);
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

        private static bool IsAllZeros(string hex)
        {
            foreach (var c in hex)
            {
                if (c != '0')
                    return false;
            }

            return true;
        }

        private static string GenerateSpanId()
        {
            return GenerateHexIdentifier(8);
        }

        private static string GenerateHexIdentifier(int byteCount)
        {
            var bytes = new byte[byteCount];
            string value;
            using (var random = RandomNumberGenerator.Create())
            {
                do
                {
                    random.GetBytes(bytes);
                    value = BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant();
                }
                while (IsAllZeros(value));
            }

            return value;
        }
    }
}
