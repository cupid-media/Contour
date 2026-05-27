using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Contour.Receiving;
using Contour.Tracing;
using FluentAssertions;
using Moq;
using NUnit.Framework;

namespace Contour.RabbitMq.Tests
{
    [TestFixture]
    public class TracingReplySpecs
    {
        [Test]
        public void Should_generate_new_span_id_when_replying_with_trace_context()
        {
            // Arrange
            var originalTraceId = "12345678901234567890123456789012";
            var originalSpanId = "1234567890123456";
            var originalTraceFlags = (byte)0x01;
            var originalTraceParent = $"00-{originalTraceId}-{originalSpanId}-{originalTraceFlags:x2}";
            var originalTraceState = "rojo=00f067aa0ba902b7";

            var headers = new Dictionary<string, object>
            {
                [Headers.TraceParent] = Encoding.UTF8.GetBytes(originalTraceParent),
                [Headers.TraceState] = Encoding.UTF8.GetBytes(originalTraceState)
            };

            var incomingMessage = new Message<TestRequest>(
                MessageLabel.From("test.request"),
                headers,
                new TestRequest { Value = "test" });

            var mockDelivery = new Mock<IDelivery>();
            mockDelivery.Setup(d => d.CanReply).Returns(true);
            
            IMessage capturedReplyMessage = null;
            mockDelivery.Setup(d => d.ReplyWith(It.IsAny<IMessage>()))
                      .Callback<IMessage>(msg => capturedReplyMessage = msg);

            var mockBusContext = new Mock<IBusContext>();
            
            var consumingContext = new DefaultConsumingContext<TestRequest>(
                mockBusContext.Object, 
                incomingMessage, 
                mockDelivery.Object);

            // Act
            consumingContext.Reply(new TestResponse { Result = "response" });

            // Assert
            capturedReplyMessage.Should().NotBeNull();
            capturedReplyMessage.Headers.Should().ContainKey(Headers.TraceParent);
            capturedReplyMessage.Headers.Should().ContainKey(Headers.TraceState);

            var replyTraceParent = capturedReplyMessage.Headers[Headers.TraceParent].ToString();
            var replyTraceState = capturedReplyMessage.Headers[Headers.TraceState].ToString();

            // Verify trace parent format
            replyTraceParent.Should().StartWith("00-");
            var parts = replyTraceParent.Split('-');
            parts.Should().HaveCount(4);

            // Verify trace ID is preserved
            parts[1].Should().Be(originalTraceId);

            // Verify span ID is different (new span generated)
            parts[2].Should().NotBe(originalSpanId);
            parts[2].Should().HaveLength(16);

            // Verify trace flags are preserved
            parts[3].Should().Be(originalTraceFlags.ToString("x2"));

            // Verify trace state is preserved
            replyTraceState.Should().Be(originalTraceState);
        }

        [Test]
        public void Should_handle_string_trace_headers()
        {
            // Arrange
            var originalTraceId = "abcdef1234567890abcdef1234567890";
            var originalSpanId = "abcdef1234567890";
            var originalTraceFlags = (byte)0x01;
            var originalTraceParent = $"00-{originalTraceId}-{originalSpanId}-{originalTraceFlags:x2}";

            var headers = new Dictionary<string, object>
            {
                [Headers.TraceParent] = originalTraceParent // String instead of byte[]
            };

            var incomingMessage = new Message<TestRequest>(
                MessageLabel.From("test.request"),
                headers,
                new TestRequest { Value = "test" });

            var mockDelivery = new Mock<IDelivery>();
            mockDelivery.Setup(d => d.CanReply).Returns(true);
            
            IMessage capturedReplyMessage = null;
            mockDelivery.Setup(d => d.ReplyWith(It.IsAny<IMessage>()))
                      .Callback<IMessage>(msg => capturedReplyMessage = msg);

            var mockBusContext = new Mock<IBusContext>();
            
            var consumingContext = new DefaultConsumingContext<TestRequest>(
                mockBusContext.Object, 
                incomingMessage, 
                mockDelivery.Object);

            // Act
            consumingContext.Reply(new TestResponse { Result = "response" });

            // Assert
            capturedReplyMessage.Should().NotBeNull();
            capturedReplyMessage.Headers.Should().ContainKey(Headers.TraceParent);

            var replyTraceParent = capturedReplyMessage.Headers[Headers.TraceParent].ToString();
            var parts = replyTraceParent.Split('-');

            // Verify trace ID is preserved and span ID is different
            parts[1].Should().Be(originalTraceId);
            parts[2].Should().NotBe(originalSpanId);
        }

        [Test]
        public void Should_drop_invalid_trace_parent_when_trace_parent_parsing_fails()
        {
            // Arrange
            var invalidTraceParent = "invalid-trace-parent-format";
            var headers = new Dictionary<string, object>
            {
                [Headers.TraceParent] = Encoding.UTF8.GetBytes(invalidTraceParent)
            };

            var incomingMessage = new Message<TestRequest>(
                MessageLabel.From("test.request"),
                headers,
                new TestRequest { Value = "test" });

            var mockDelivery = new Mock<IDelivery>();
            mockDelivery.Setup(d => d.CanReply).Returns(true);
            
            IMessage capturedReplyMessage = null;
            mockDelivery.Setup(d => d.ReplyWith(It.IsAny<IMessage>()))
                      .Callback<IMessage>(msg => capturedReplyMessage = msg);

            var mockBusContext = new Mock<IBusContext>();
            
            var consumingContext = new DefaultConsumingContext<TestRequest>(
                mockBusContext.Object, 
                incomingMessage, 
                mockDelivery.Object);

            // Act
            consumingContext.Reply(new TestResponse { Result = "response" });

            // Assert
            capturedReplyMessage.Should().NotBeNull();
            capturedReplyMessage.Headers.Should().NotContainKey(Headers.TraceParent);
        }

        [Test]
        public void Should_generate_new_span_id_from_copied_outgoing_trace_context()
        {
            var originalTraceId = "12345678901234567890123456789012";
            var originalSpanId = "1234567890123456";
            var originalTraceFlags = (byte)0x01;
            var originalTraceParent = $"00-{originalTraceId}-{originalSpanId}-{originalTraceFlags:x2}";
            var originalTraceState = "rojo=00f067aa0ba902b7";

            var outgoingHeaders = new Dictionary<string, object>
            {
                [Headers.TraceParent] = Encoding.UTF8.GetBytes(originalTraceParent),
                [Headers.TraceState] = Encoding.UTF8.GetBytes(originalTraceState)
            };

            W3CTraceContextProvider.InjectTraceContext(outgoingHeaders, null);

            var traceParent = outgoingHeaders[Headers.TraceParent].ToString();
            var parts = traceParent.Split('-');

            parts.Should().HaveCount(4);
            parts[1].Should().Be(originalTraceId);
            parts[2].Should().NotBe(originalSpanId);
            parts[2].Should().HaveLength(16);
            parts[3].Should().Be(originalTraceFlags.ToString("x2"));
            outgoingHeaders[Headers.TraceState].Should().Be(originalTraceState);
        }

        [Test]
        public void Should_not_create_root_trace_context_for_standalone_outgoing_message()
        {
            var outgoingHeaders = new Dictionary<string, object>();

            W3CTraceContextProvider.InjectTraceContext(outgoingHeaders, null);

            outgoingHeaders.Should().NotContainKey(Headers.TraceParent);
            outgoingHeaders.Should().NotContainKey(Headers.TraceState);
        }

        [Test]
        public void Should_prefer_current_activity_over_copied_outgoing_trace_context()
        {
            var copiedTraceParent = "00-12345678901234567890123456789012-1234567890123456-01";
            var outgoingHeaders = new Dictionary<string, object>
            {
                [Headers.TraceParent] = copiedTraceParent
            };

            var activity = new Activity("test");
            activity.SetIdFormat(ActivityIdFormat.W3C);
            activity.Start();

            try
            {
                W3CTraceContextProvider.InjectTraceContext(outgoingHeaders, null);

                outgoingHeaders[Headers.TraceParent].Should().Be(
                    $"00-{activity.TraceId}-{activity.SpanId}-{(byte)activity.ActivityTraceFlags:x2}");
            }
            finally
            {
                activity.Stop();
            }
        }

        private class TestRequest
        {
            public string Value { get; set; }
        }

        private class TestResponse
        {
            public string Result { get; set; }
        }
    }
}
