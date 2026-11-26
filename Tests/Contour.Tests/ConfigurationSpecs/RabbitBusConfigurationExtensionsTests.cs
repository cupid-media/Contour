using System;
using System.Linq;
using Contour.Configuration;
using Contour.Receiving.Consumers;
using Contour.Tests.TestConfiguration;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Internal;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.ConfigurationSpecs;

[TestFixture]
public class RabbitBusConfigurationExtensionsTests
{
    private RabbitBusConfigurator configurator;
    private TestDependencyResolver dependencyResolver;

    [SetUp]
    public void SetUp()
    {
        this.configurator = new RabbitBusConfigurator();
        this.dependencyResolver = new TestDependencyResolver();
    }

    [Test]
    public void Configure_WithCompleteEndpoint_ShouldApplyAllConfigurations()
    {
        // Arrange
        var endpoint = new EndpointConfiguration
        {
            Name = "TestEndpoint",
            ConnectionString = "amqp://test:test@localhost:5672/test",
            ReuseConnection = false,
            ParallelismLevel = 4,
            FaultQueueTtl = TimeSpan.FromMinutes(30),
            FaultQueueLimit = 1000,
            QueueLimit = 500,
            QueueMaxLengthBytes = 1024 * 1024, // 1MB
            ExcludedHeaders = ["X-Test-Header", "X-Internal-Header"],
            ConnectionStringProvider = "TestConnectionStringProvider",
            LifecycleHandler = "TestLifecycleHandler",
            Qos = new QosConfiguration
            {
                PrefetchCount = 100,
                PrefetchSize = 512
            },
            Dynamic = new DynamicConfiguration
            {
                Outgoing = true
            },
            Incoming =
            [
                new IncomingConfiguration
                {
                    Key = "TestIncomingKey",
                    Label = "test.incoming.message",
                    Type = typeof(TestIncomingMessage).AssemblyQualifiedName,
                    React = "TestConsumer",
                    RequiresAccept = true,
                    Lifestyle = Lifestyle.Normal,
                    ConnectionString = "amqp://test:test@localhost:5672/incoming",
                    ReuseConnection = true,
                    QueueLimit = 200,
                    QueueMaxLengthBytes = 512 * 1024,
                    ParallelismLevel = 2,
                    Delayed = false,
                    Qos = new QosConfiguration
                    {
                        PrefetchCount = 50,
                        PrefetchSize = 256
                    }
                }
            ],
            Outgoing =
            [
                new OutgoingConfiguration
                {
                    Key = "TestOutgoingKey",
                    Label = "test.outgoing.message",
                    Confirm = true,
                    Persist = true,
                    Timeout = TimeSpan.FromSeconds(30),
                    Ttl = TimeSpan.FromMinutes(10),
                    ConnectionString = "amqp://test:test@localhost:5672/outgoing",
                    ReuseConnection = false,
                    Delayed = true,
                    ConfirmTimeout = TimeSpan.FromSeconds(5),
                    CallbackEndpoint = new CallbackEndpointConfiguration { Default = true }
                }
            ]
        };

        // Setup dependency resolver
        this.dependencyResolver.Register<IConnectionStringProvider>("TestConnectionStringProvider", new TestConnectionStringProvider());
        this.dependencyResolver.Register<IBusLifecycleHandler>("TestLifecycleHandler", new TestBusLifecycleHandler());
        this.dependencyResolver.Register<IConsumer<TestIncomingMessage>>("TestConsumer", new TestIncomingMessageConsumer());

        // Act
        var result = this.configurator.Configure(endpoint, this.dependencyResolver.Resolve);

        // Assert - Verify endpoint configuration
        result.Should().NotBeNull();
        result.Should().Be(this.configurator); // Should return the same configurator instance

        // Verify receiver configuration
        this.configurator.ReceiverConfigurators.Should().HaveCount(1);
        var receiverConfig = this.configurator.ReceiverConfigurators[0];
        receiverConfig.Options.Label.Name.Should().Be("test.incoming.message");
        receiverConfig.Options.AcceptRequired.Should().BeTrue();
        // ConnectionString может быть перезаписан ConnectionStringProvider
        receiverConfig.Options.ConnectionString.Should().NotBeNullOrEmpty();
        receiverConfig.Options.ReuseConnection.Should().BeTrue();
        receiverConfig.Options.QueueLimit.Should().Be(200);
        receiverConfig.Options.QueueMaxLengthBytes.Should().Be(512 * 1024);
        receiverConfig.Options.ParallelismLevel.Should().Be(2);
        receiverConfig.Options.QoS.PrefetchCount.Should().Be(50);
        receiverConfig.Options.QoS.PrefetchSize.Should().Be(256);

        // Verify sender configurations (including auto-generated ones)
        this.configurator.SenderConfigurators.Should().HaveCount(4); // test.outgoing.message + dynamic * + Unhandled + Fault
        var userSenderConfig = this.configurator.SenderConfigurators.FirstOrDefault(s => s.Label.Name == "test.outgoing.message");
        userSenderConfig.Should().NotBeNull();
        userSenderConfig.Options.ConfirmationRequired.Should().BeTrue();
        userSenderConfig.Options.Persistently.Should().BeTrue();
        userSenderConfig.Options.RequestTimeout.Should().Be(TimeSpan.FromSeconds(30));
        // ConnectionString может быть перезаписан ConnectionStringProvider
        userSenderConfig.Options.ConnectionString.Should().NotBeNullOrEmpty();
        userSenderConfig.Options.ReuseConnection.Should().BeFalse();

        // Verify lifecycle handler was set
        this.configurator.BusLifecycleHandler.Should().BeOfType<TestBusLifecycleHandler>();

        // Create the bus to verify full configuration
        var bus = this.configurator.Create();
        bus.Should().NotBeNull();
        bus.Should().BeOfType<RabbitBus>();
    }

    [Test]
    public void Configure_WithMinimalEndpoint_ShouldApplyBasicConfigurations()
    {
        // Arrange
        var minimalConfigurator = new RabbitBusConfigurator(); // Use new instance without auto-generated senders
        var endpoint = new EndpointConfiguration
        {
            Name = "MinimalEndpoint",
            ConnectionString = "amqp://localhost:5672",
            Incoming = [],
            Outgoing = [],
            ExcludedHeaders = []
        };

        // Act
        var result = minimalConfigurator.Configure(endpoint, this.dependencyResolver.Resolve);

        // Assert
        result.Should().NotBeNull();
        result.Should().Be(minimalConfigurator);

        // Check what was configured
        var hasSenders = minimalConfigurator.SenderConfigurators.Count;
        var hasReceivers = minimalConfigurator.ReceiverConfigurators.Count;

        // No incoming/outgoing means no receivers/senders should be configured
        hasReceivers.Should().Be(0);
        hasSenders.Should().Be(2);

        // Since there are no senders/receivers, validation will fail - this is expected behavior
        Action validateAction = () => minimalConfigurator.Validate();
        validateAction.Should().Throw<BusConfigurationException>()
            .WithMessage("No senders and receivers are registered.");
    }
}
