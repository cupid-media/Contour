using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Contour.Helpers;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

/// <summary>
/// The message specs.
/// </summary>
// ReSharper disable InconsistentNaming
[SuppressMessage("StyleCop.CSharp.NamingRules", "SA1300:ElementMustBeginWithUpperCaseLetter", Justification = "Reviewed. Suppression is OK here.")]
public class MessageSpecs
{
    public record TestMessage(string Message);

    [TestFixture]
    [Category("Unit")]
    public class when_creating_untyped_message
    {
        [Test]
        public void should_create_message_with_defined_state()
        {
            var message = new Message<TestMessage>(
                "boo".ToMessageLabel(),
                new TestMessage("Body"), new Dictionary<string, object> { { "This", "That" } });

            message.Label.Name.Should().Be("boo");
            message.Headers.Should().HaveCount(1);
            message.Headers.Should().ContainKey("This");
            message.Payload.Message.Should().Be("Body");
        }
    }

    [TestFixture]
    [Category("Unit")]
    public class when_cloning_untyped_message_with_new_label
    {
        [Test]
        public void should_create_message_with_combined_state()
        {
            var original = new Message<TestMessage>(
                "boo".ToMessageLabel(),
                new TestMessage("Body"), new Dictionary<string, object> { { "This", "That" } });

            var target = original with
            {
                Label = "moo".ToMessageLabel()
            };

            target.Should().BeOfType<Message<TestMessage>>();
            target.Label.Name.Should().Be("moo");
            target.Headers.Should().HaveCount(1);
            target.Headers.Should().ContainKey("This");
            target.Payload.Message.Should().Be("Body");
        }
    }

    [TestFixture]
    [Category("Unit")]
    public class when_cloning_untyped_message_with_new_payload
    {
        [Test]
        public void should_create_message_with_combined_state()
        {
            var original = new Message<TestMessage>(
                "boo".ToMessageLabel(),
                new TestMessage("Body"), new Dictionary<string, object> { { "This", "That" } });

            var target = original with
            {
                Payload = new TestMessage("Zzzz")
            };

            target.Should().BeOfType<Message<TestMessage>>();
            target.Label.Name.Should().Be("boo");
            target.Headers.Should().HaveCount(1);
            target.Headers.Should().ContainKey("This");
            target.Payload.Message.Should().Be("Zzzz");
        }
    }
}