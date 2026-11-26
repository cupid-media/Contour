using System;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Configuration;

namespace Contour.Tests.TestConfiguration;

public class TestEndpoint
{
    public static EndpointConfiguration Default => new()
    {
        Name = "Endpoint",
        ConnectionString = "amqp://localhost:5672",
        ReuseConnection = true,
        ParallelismLevel = 2,
        FaultQueueTtl = TimeSpan.FromMinutes(5),
        FaultQueueLimit = 100,
        QueueLimit = 50,
        ExcludedHeaders = ["X-Test-Header"],
        Incoming =
        [
            new IncomingConfiguration
            {
                Key = "TestIncomingKey",
                Label = "in.test.message",
                Type = typeof(TestIncomingMessage).AssemblyQualifiedName,
                React = "TestConsumer",
                RequiresAccept = false,
                Lifestyle = Lifestyle.Normal,
                ConnectionString = "amqp://localhost:5672/incoming",
                ReuseConnection = true,
                QueueLimit = 25,
                ParallelismLevel = 1,
                Delayed = false
            }
        ],
        Outgoing =
        [
            new OutgoingConfiguration
            {
                Key = "TestOutgoingKey",
                Label = "test.response",
                Confirm = false,
                Persist = false,
                Timeout = TimeSpan.FromSeconds(10),
                ConnectionString = "amqp://localhost:5672/outgoing",
                ReuseConnection = true,
                Delayed = false,
                CallbackEndpoint = new CallbackEndpointConfiguration { Default = true }
            }
        ],
        Qos = new QosConfiguration
        {
            PrefetchCount = 10
        }
    };
}
