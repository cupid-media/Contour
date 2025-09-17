using System;
using System.Collections.Generic;
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
        public void Should_fallback_to_original_when_trace_parent_parsing_fails()
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
            capturedReplyMessage.Headers.Should().ContainKey(Headers.TraceParent);

            var replyTraceParent = capturedReplyMessage.Headers[Headers.TraceParent].ToString();
            replyTraceParent.Should().Be(invalidTraceParent);
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
