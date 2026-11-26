using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving;
using FluentAssertions;
using Moq;
using NUnit.Framework;

namespace Contour.Tests.Common;

[TestFixture]
[Category("Unit")]
public class TracingReplySpecs
{
    [Test]
    public async Task Should_generate_new_span_id_when_replying_with_trace_context()
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

        // Act
        var replyMessage = await ReplyTo(headers);

        // Assert
        replyMessage.Should().NotBeNull();
        replyMessage.Headers.Should().ContainKey(Headers.TraceParent);
        replyMessage.Headers.Should().ContainKey(Headers.TraceState);

        var replyTraceParent = replyMessage.Headers[Headers.TraceParent].ToString();
        var replyTraceState = replyMessage.Headers[Headers.TraceState].ToString();

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
    public async Task Should_handle_string_trace_headers()
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

        // Act
        var replyMessage = await ReplyTo(headers);

        // Assert
        replyMessage.Should().NotBeNull();
        replyMessage.Headers.Should().ContainKey(Headers.TraceParent);

        var replyTraceParent = replyMessage.Headers[Headers.TraceParent].ToString();
        var parts = replyTraceParent.Split('-');

        // Verify trace ID is preserved and span ID is different
        parts[1].Should().Be(originalTraceId);
        parts[2].Should().NotBe(originalSpanId);
    }

    [Test]
    public async Task Should_fallback_to_original_when_trace_parent_parsing_fails()
    {
        // Arrange
        var invalidTraceParent = "invalid-trace-parent-format";
        var headers = new Dictionary<string, object>
        {
            [Headers.TraceParent] = Encoding.UTF8.GetBytes(invalidTraceParent)
        };

        // Act
        var replyMessage = await ReplyTo(headers);

        // Assert
        replyMessage.Should().NotBeNull();
        replyMessage.Headers.Should().ContainKey(Headers.TraceParent);

        var replyTraceParent = replyMessage.Headers[Headers.TraceParent].ToString();
        replyTraceParent.Should().Be(invalidTraceParent);
    }

    /// <summary>
    /// Replies to a request that came with <paramref name="requestHeaders"/> and returns the reply message.
    /// </summary>
    private static async Task<Message<TestResponse>> ReplyTo(IDictionary<string, object> requestHeaders)
    {
        var incomingMessage = new Message<TestRequest>(
            MessageLabel.From("test.request"),
            new TestRequest { Value = "test" },
            requestHeaders);

        Message<TestResponse> capturedReplyMessage = null;

        var mockDelivery = new Mock<IDelivery>();
        mockDelivery.Setup(d => d.CanReply).Returns(true);
        mockDelivery
            .Setup(d => d.ReplyWithAsync(It.IsAny<Message<TestResponse>>(), It.IsAny<CancellationToken>()))
            .Callback<Message<TestResponse>, CancellationToken>((message, _) => capturedReplyMessage = message)
            .Returns(Task.CompletedTask);

        var consumingContext = new DefaultConsumingContext<TestRequest>(
            new Mock<IBus>().Object,
            incomingMessage,
            mockDelivery.Object);

        await consumingContext.ReplyAsync(new TestResponse { Result = "response" });

        return capturedReplyMessage;
    }

    public class TestRequest
    {
        public string Value { get; set; }
    }

    public class TestResponse
    {
        public string Result { get; set; }
    }
}
