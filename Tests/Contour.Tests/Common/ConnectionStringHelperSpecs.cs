using Contour.Helpers;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

[TestFixture]
[Category("Unit")]
public class ConnectionStringHelperSpecs
{
    [TestCase("amqp://user:secret@rabbit.local:5672/vhost", "amqp://rabbit.local:5672/vhost")]
    [TestCase("amqps://user:secret@rabbit.local/vhost", "amqps://rabbit.local/vhost")]
    [TestCase("amqps://user:secret@rabbit.local", "amqps://rabbit.local/")]
    [TestCase("amqp://rabbit.local:5672/", "amqp://rabbit.local:5672/")]
    [TestCase("amqp://user:p%40ss@rabbit.local/vhost?heartbeat=10", "amqp://rabbit.local/vhost")]
    public void Should_remove_credentials_and_query(string connectionString, string expected)
    {
        ConnectionStringHelper.Sanitize(connectionString).Should().Be(expected);
    }

    [Test]
    public void Should_sanitize_every_url_of_a_multi_broker_connection_string()
    {
        var sanitized = ConnectionStringHelper.Sanitize(
            "amqp://user:secret@rabbit1.local:5672/vhost, amqp://other:hidden@rabbit2.local:5672/vhost");

        sanitized.Should().Be("amqp://rabbit1.local:5672/vhost,amqp://rabbit2.local:5672/vhost");
    }

    [TestCase("user:secret@rabbit.local")]
    [TestCase("not a url")]
    public void Should_hide_a_string_that_is_not_an_absolute_url(string connectionString)
    {
        var sanitized = ConnectionStringHelper.Sanitize(connectionString);

        sanitized.Should().NotContain("secret");
        sanitized.Should().NotContain("not a url");
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("  ")]
    public void Should_return_an_empty_value_as_is(string connectionString)
    {
        ConnectionStringHelper.Sanitize(connectionString).Should().Be(connectionString);
    }
}
