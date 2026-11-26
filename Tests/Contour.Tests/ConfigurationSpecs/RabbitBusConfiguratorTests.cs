using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Sending;
using Contour.Serialization;
using Contour.Transport.RabbitMQ;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Internal;
using FluentAssertions;
using Moq;
using NUnit.Framework;

namespace Contour.Tests.ConfigurationSpecs;

[TestFixture]
public class RabbitBusConfiguratorTests
{
    private RabbitBusConfigurator configurator;
    private Mock<IConnectionStringProvider> connectionStringProviderMock;
    private Mock<IBusLifecycleHandler> busLifecycleHandlerMock;
    private Mock<IPayloadConverter> payloadConverterMock;
    private Mock<IIncomingMessageHeaderStorage> messageHeaderStorageMock;
    private Mock<IFailedDeliveryStrategy> failedDeliveryStrategyMock;
    private Mock<IUnhandledDeliveryStrategy> unhandledDeliveryStrategyMock;

    [SetUp]
    public void SetUp()
    {
        this.configurator = new RabbitBusConfigurator();
        this.connectionStringProviderMock = new Mock<IConnectionStringProvider>();
        this.busLifecycleHandlerMock = new Mock<IBusLifecycleHandler>();
        this.payloadConverterMock = new Mock<IPayloadConverter>();
        this.messageHeaderStorageMock = new Mock<IIncomingMessageHeaderStorage>();
        this.failedDeliveryStrategyMock = new Mock<IFailedDeliveryStrategy>();
        this.unhandledDeliveryStrategyMock = new Mock<IUnhandledDeliveryStrategy>();
    }

    [Test]
    public void CompleteConfiguration_ShouldSetAllOptionsCorrectly()
    {
        // Arrange
        const string endpointName = "TestEndpoint";
        const string connectionString = "amqp://test:test@localhost:5672/test";
        const bool reuseConnection = false;
        const int failoverAttempts = 5;
        var excludedHeaders = new List<string> { "X-Test-Header", "X-Internal-Header" };
        const uint parallelismLevel = 8;
        var faultQueueTtl = TimeSpan.FromMinutes(30);
        const int faultQueueLimit = 1000;
        const int queueLimit = 500;
        const int queueMaxLengthBytes = 1024 * 1024; // 1MB
        var qosParams = new QoSParams(100, 512);
        var requestTimeout = TimeSpan.FromSeconds(30);

        var routeResolverBuilder = new Func<RabbitRouteResolverBuilderParams, CancellationToken, ValueTask<IRouteResolver>>(
            (builder, _) => ValueTask.FromResult(Mock.Of<IRouteResolver>()));

        // Test messages
        var testMessageLabel = MessageLabel.From("test.message");
        var emitMessageLabel = MessageLabel.From("test.request");
        var requestMessageLabel = MessageLabel.From("test.response");

        // Act - Configure the bus with various settings
        this.configurator
            .WithEndpointName(endpointName)
            .WithConnectionStringProvider(this.connectionStringProviderMock.Object)
            .WithBusLifecycleHandler(this.busLifecycleHandlerMock.Object)
            .WithPayloadConverter(this.payloadConverterMock.Object)
            .WithIncomingMessageHeaderStorage(this.messageHeaderStorageMock.Object)
            .WithConnectionString(connectionString)
            .WithReuseConnection(reuseConnection)
            .WithFailoverAttempts(failoverAttempts)
            .WithExcludedHeaders(excludedHeaders)
            .WithParallelismLevel(parallelismLevel)
            .WithFaultQueueTtl(faultQueueTtl)
            .WithFaultQueueLimit(faultQueueLimit)
            .WithQueueLimit(queueLimit)
            .WithQueueMaxLengthBytes(queueMaxLengthBytes)
            .WithQoS(qosParams)
            .WithRouteResolverBuilder(routeResolverBuilder)
            .UseDefaultFaultStrategy()
            .OnFailed(this.failedDeliveryStrategyMock.Object)
            .OnUnhandled(this.unhandledDeliveryStrategyMock.Object);

        // Configure a receiver using consumer factory
        this.configurator.On(testMessageLabel)
            .ReactWith(() => new TestMessageConsumer());

        this.configurator.On("test.message.custom")
            .ReactWith(() => new TestMessageConsumer())
            .RequiresAccept()
            .WithParallelismLevel(4)
            .WithQueueLimit(200)
            .WithConnectionString("amqp://custom:custom@localhost:5672/test");

        // Configure a sender
        this.configurator.Route(emitMessageLabel);

        // Configure a request-response sender
        this.configurator.Route(requestMessageLabel)
            .Persistently()
            .WithRequestTimeout(TimeSpan.FromSeconds(45))
            .WithConnectionString("amqp://custom:custom@localhost:5672/test")
            .WithTtl(TimeSpan.FromSeconds(10))
            .WithConfirmation()
            .WithDefaultCallbackEndpoint();

        // Assert - Verify configurator properties
        this.configurator.AutoStart.Should().BeTrue();
        this.configurator.BusLifecycleHandler.Should().Be(this.busLifecycleHandlerMock.Object);
        this.configurator.PayloadConverter.Should().Be(this.payloadConverterMock.Object);
        this.configurator.SenderConfigurators.Should().HaveCount(4); // 2 for configured Route + SystemQueues.Unhandled + SystemQueues.Fault
        this.configurator.ReceiverConfigurators.Should().HaveCount(2);

        // Assert - Verify receiver configuration
        var receiverConfig = this.configurator.ReceiverConfigurators.First(c => c.Label == testMessageLabel);
        receiverConfig.Options.Label.Should().Be(testMessageLabel);
        receiverConfig.Options.AcceptRequired.Should().BeFalse();
        receiverConfig.Options.ParallelismLevel.Should().Be(parallelismLevel);
        receiverConfig.Options.QueueLimit.Should().Be(queueLimit);
        receiverConfig.Options.ConnectionString.Should().Be(connectionString);
        receiverConfig.Options.ReuseConnection.Should().Be(reuseConnection);
        receiverConfig.Options.ExcludedHeaders.Should().BeEquivalentTo(excludedHeaders);
        receiverConfig.Options.QueueMaxLengthBytes.Should().Be(queueMaxLengthBytes);
        receiverConfig.Options.QoS.Should().Be(qosParams);
        receiverConfig.Options.FailedDeliveryStrategy.Should().Be(this.failedDeliveryStrategyMock.Object);
        receiverConfig.Options.UnhandledDeliveryStrategy.Should().Be(this.unhandledDeliveryStrategyMock.Object);
        receiverConfig.Options.ConnectionStringProvider.Should().Be(this.connectionStringProviderMock.Object);
        receiverConfig.Options.IncomingMessageHeaderStorage.Should().Be(this.messageHeaderStorageMock.Object);

        var receiverConfig2 = this.configurator.ReceiverConfigurators.First(c => c.Label == MessageLabel.From("test.message.custom"));
        receiverConfig2.Options.Label.Should().Be(MessageLabel.From("test.message.custom"));
        receiverConfig2.Options.AcceptRequired.Should().BeTrue();
        receiverConfig2.Options.ParallelismLevel.Should().Be(4);
        receiverConfig2.Options.QueueLimit.Should().Be(200);
        receiverConfig2.Options.ConnectionString.Should().Be("amqp://custom:custom@localhost:5672/test");
        receiverConfig2.Options.ReuseConnection.Should().Be(reuseConnection);
        receiverConfig2.Options.ExcludedHeaders.Should().BeEquivalentTo(excludedHeaders);
        receiverConfig2.Options.QueueMaxLengthBytes.Should().Be(queueMaxLengthBytes);
        receiverConfig2.Options.QoS.Should().Be(qosParams);
        receiverConfig2.Options.FailedDeliveryStrategy.Should().Be(this.failedDeliveryStrategyMock.Object);
        receiverConfig2.Options.UnhandledDeliveryStrategy.Should().Be(this.unhandledDeliveryStrategyMock.Object);
        receiverConfig2.Options.ConnectionStringProvider.Should().Be(this.connectionStringProviderMock.Object);
        receiverConfig2.Options.IncomingMessageHeaderStorage.Should().Be(this.messageHeaderStorageMock.Object);

        // Assert - Verify sender configurations
        var emitSenderConfig = this.configurator.SenderConfigurators.First(s => s.Label == emitMessageLabel);
        emitSenderConfig.Label.Should().Be(emitMessageLabel);
        emitSenderConfig.Options.Persistently.Should().BeFalse();
        emitSenderConfig.Options.RequiresCallback.Should().BeFalse();
        emitSenderConfig.Options.ConfirmationRequired.Should().BeFalse();
        emitSenderConfig.Options.RequestTimeout.Should().Be(requestTimeout);
        emitSenderConfig.Options.ConnectionString.Should().Be(connectionString);
        emitSenderConfig.Options.CallbackOptions.ConnectionString.Should().Be(connectionString);
        emitSenderConfig.Options.ReuseConnection.Should().Be(reuseConnection);
        emitSenderConfig.Options.FailoverAttempts.Should().Be(failoverAttempts);
        emitSenderConfig.Options.RouteResolverBuilder.Should().Be(routeResolverBuilder);
        emitSenderConfig.Options.ConnectionStringProvider.Should().Be(this.connectionStringProviderMock.Object);
        emitSenderConfig.Options.IncomingMessageHeaderStorage.Should().Be(this.messageHeaderStorageMock.Object);

        var requestSenderConfig = this.configurator.SenderConfigurators.First(s => s.Label == requestMessageLabel);
        requestSenderConfig.Label.Should().Be(requestMessageLabel);
        requestSenderConfig.Options.RequestTimeout.Should().Be(TimeSpan.FromSeconds(45));
        requestSenderConfig.Options.Ttl.Should().Be(TimeSpan.FromSeconds(10));
        requestSenderConfig.Options.RequiresCallback.Should().BeTrue();
        requestSenderConfig.Options.ConfirmationRequired.Should().BeTrue();
        requestSenderConfig.Options.Persistently.Should().BeTrue();
        requestSenderConfig.Options.ConnectionString.Should().Be("amqp://custom:custom@localhost:5672/test");
        requestSenderConfig.Options.CallbackOptions.ConnectionString.Should().Be("amqp://custom:custom@localhost:5672/test");
        requestSenderConfig.Options.ReuseConnection.Should().Be(reuseConnection);
        requestSenderConfig.Options.FailoverAttempts.Should().Be(failoverAttempts);
        requestSenderConfig.Options.RouteResolverBuilder.Should().Be(routeResolverBuilder);
        requestSenderConfig.Options.ConnectionStringProvider.Should().Be(this.connectionStringProviderMock.Object);
        requestSenderConfig.Options.IncomingMessageHeaderStorage.Should().Be(this.messageHeaderStorageMock.Object);

        // Assert - Validate() should not throw
        Action validateAction = () => this.configurator.Validate();
        validateAction.Should().NotThrow();
    }

    [Test]
    public void ParameterValidation_ShouldThrowExceptions_ForInvalidValues()
    {
        // Arrange & Act & Assert
        this.configurator.Invoking(c => c.WithFailoverAttempts(-1))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("failoverAttempts");

        this.configurator.Invoking(c => c.WithParallelismLevel(0))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("parallelismLevel");

        this.configurator.Invoking(c => c.WithFaultQueueTtl(TimeSpan.Zero))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("faultQueueTtl");

        this.configurator.Invoking(c => c.WithFaultQueueTtl(TimeSpan.FromSeconds(-1)))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("faultQueueTtl");

        this.configurator.Invoking(c => c.WithFaultQueueLimit(0))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("faultQueueLimit");

        this.configurator.Invoking(c => c.WithFaultQueueLimit(-1))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("faultQueueLimit");

        this.configurator.Invoking(c => c.WithQueueLimit(0))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("queueLimit");

        this.configurator.Invoking(c => c.WithQueueMaxLengthBytes(-1))
            .Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("queueMaxLengthBytes");
    }

    [Test]
    public void DuplicateRegistration_ShouldThrowException()
    {
        // Arrange
        const string messageLabel = "test.duplicate";

        // Act
        this.configurator.On(messageLabel).ReactWith(() => new TestMessageConsumer());

        // Assert
        this.configurator.Invoking(c => c.On(messageLabel))
            .Should().Throw<ArgumentException>()
            .WithMessage($"Receiver for label [{messageLabel}] is already registered*");

        this.configurator.Route(messageLabel);
        this.configurator.Invoking(c => c.Route(messageLabel))
            .Should().Throw<ArgumentException>()
            .WithMessage($"Sender for label [{messageLabel}] already registered.*");
    }

    [Test]
    public void GenericMethods_ShouldResolveMessageLabels()
    {
        // Act
        this.configurator.On<TestMessage>().ReactWith(() => new TestMessageConsumer());
        this.configurator.Route<TestRequest>();
        this.configurator.UseDefaultFaultStrategy();

        // Assert
        this.configurator.ReceiverConfigurators.Should().HaveCount(1);
        this.configurator.SenderConfigurators.Should().HaveCount(3); // 1 for Route + SystemQueues.Unhandled + SystemQueues.Fault

        // Validate labels are resolved correctly
        var receiverLabel = this.configurator.ReceiverConfigurators[0].Options.Label;
        var userSenderConfig = this.configurator.SenderConfigurators.FirstOrDefault(s => s.Label.Name == "test.request");

        receiverLabel.Should().NotBeNull();
        receiverLabel.Name.Should().Be("test.message");
        userSenderConfig.Should().NotBeNull();
        userSenderConfig.Label.Name.Should().Be("test.request");
    }

    [Test]
    public void DefaultConfiguration_ShouldHaveAutoGeneratedFaultHandlersButStateNotValid()
    {
        // Arrange
        var emptyConfigurator = new RabbitBusConfigurator();

        // Act & Assert
        emptyConfigurator.SenderConfigurators.Should().HaveCount(0);
        emptyConfigurator.ReceiverConfigurators.Should().HaveCount(0);

        // Validate should still not throw because fault handlers are considered valid
        emptyConfigurator.Invoking(c => c.Validate()).Should().Throw<BusConfigurationException>()
            .WithMessage("No senders and receivers are registered.");
    }

    [Test]
    public void ConfigurationOverride_GenericConfigurationShouldBeSetBeforeConfiguringRoutes()
    {
        this.configurator.WithConnectionString("connectionstring1");

        var test1Configuration = this.configurator.On("test1");
        var test2Configuration = this.configurator.On("test2")
            .WithConnectionString("connectionstring2");

        this.configurator.WithConnectionString("connectionstring3");

        var test3Configuration = this.configurator.On("test3");

        test1Configuration.Options.ConnectionString.Should().Be("connectionstring1");
        test2Configuration.Options.ConnectionString.Should().Be("connectionstring2");
        test3Configuration.Options.ConnectionString.Should().Be("connectionstring3");
    }

    [Test]
    public void Create_ShouldReturnConfiguredBus()
    {
        // Arrange
        this.configurator
            .On("test.message").ReactWith(() => new TestMessageConsumer());

        // Act
        var bus = this.configurator.Create();

        // Assert
        bus.Should().NotBeNull();
        bus.Should().BeOfType<RabbitBus>();
    }

    [Test]
    public void Create_ShouldReturnBusReportingTheRmqTransport()
    {
        this.configurator
            .On("test.message").ReactWith(() => new TestMessageConsumer());

        var bus = this.configurator.Create();

        bus.Transport.Should().Be(BusTransports.Rmq);
    }

    // Test message classes and consumer
    [Message("test.message")]
    private class TestMessage
    {
        public string Content { get; set; }
    }

    [Message("test.request")]
    private class TestRequest
    {
        public string RequestData { get; set; }
    }

    private class TestMessageConsumer : IConsumerOf<TestMessage>
    {
        public Task HandleAsync(IConsumingContext<TestMessage> context)
        {
            // Test implementation
            return Task.CompletedTask;
        }
    }
}
