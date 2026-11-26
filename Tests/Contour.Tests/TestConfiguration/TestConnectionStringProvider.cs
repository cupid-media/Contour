using Contour.Configuration;

namespace Contour.Tests.TestConfiguration;

public class TestConnectionStringProvider : IConnectionStringProvider
{
    public string GetConnectionString(MessageLabel messageLabel)
    {
        return "amqp://test-provider:test@localhost:5672/test";
    }
}
