using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Contour.Configuration;
using Contour.Sending;
using Contour.Serialization;
using Contour.Transport.RabbitMQ;
using Contour.Transport.RabbitMQ.Internal;
using FluentAssertions;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Contour.UnitTests
{
    [TestFixture]
    public class ProducerShutdownSpecs
    {
        private static (Producer producer, Mock<IModel> modelMock, Mock<IRabbitConnection> connectionMock) CreateProducer(bool modelIsOpen = true)
        {
            var isOpen = modelIsOpen;

            var payloadConverter = new Mock<IPayloadConverter>();
            payloadConverter.Setup(c => c.FromObject(It.IsAny<object>())).Returns(Encoding.UTF8.GetBytes("{}"));
            payloadConverter.SetupGet(c => c.ContentType).Returns("application/json");

            var busContext = new Mock<IBusContext>();
            busContext.SetupGet(b => b.Endpoint).Returns(new Endpoint("test-endpoint"));
            busContext.SetupGet(b => b.MessageLabelHandler).Returns(new DefaultRabbitMessageLabelHandler());
            busContext.SetupGet(b => b.PayloadConverter).Returns(payloadConverter.Object);

            var propsMock = new Mock<IBasicProperties>();
            propsMock.SetupAllProperties();
            propsMock.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>());

            var modelMock = new Mock<IModel>();
            modelMock.SetupGet(m => m.IsOpen).Returns(() => isOpen);
            modelMock.Setup(m => m.CreateBasicProperties()).Returns(propsMock.Object);
            modelMock.Setup(m => m.Abort()).Callback(() => isOpen = false);
            modelMock.Setup(m => m.Dispose()).Callback(() => isOpen = false);

            var connectionMock = new Mock<IRabbitConnection>();
            connectionMock.SetupGet(c => c.ConnectionString).Returns("amqp://guest:guest@localhost:5672/vhost");
            connectionMock.SetupGet(c => c.ConnectionKey).Returns("127.0.0.1:5672:vhost");
            connectionMock.SetupGet(c => c.IsBlocked).Returns(false);
            connectionMock
                .Setup(c => c.OpenChannel(It.IsAny<CancellationToken>()))
                .Returns(() => new RabbitChannel(Guid.NewGuid(), modelMock.Object, busContext.Object, "amqp://guest:guest@localhost:5672/vhost", "127.0.0.1:5672:vhost"));

            var routeResolver = new Mock<IRouteResolver>();
            routeResolver
                .Setup(r => r.Resolve(It.IsAny<IEndpoint>(), It.IsAny<MessageLabel>()))
                .Returns(new RabbitRoute("test.exchange", string.Empty));

            var producer = new Producer(
                new Endpoint("test-endpoint"),
                connectionMock.Object,
                MessageLabel.From("command.test"),
                routeResolver.Object,
                confirmationIsRequired: false);

            return (producer, modelMock, connectionMock);
        }

        [Test]
        public void Producer_ConnectionKey_should_not_throw_before_start_or_after_stop()
        {
            var (producer, _, _) = CreateProducer();

            producer.ConnectionKey.Should().Be("127.0.0.1:5672:vhost");
            producer.IsInGoodCondition.Should().BeFalse();

            producer.Start();
            producer.IsInGoodCondition.Should().BeTrue();
            producer.ConnectionKey.Should().Be("127.0.0.1:5672:vhost");

            producer.Stop();
            producer.IsInGoodCondition.Should().BeFalse();
            producer.ConnectionKey.Should().Be("127.0.0.1:5672:vhost");
        }

        [Test]
        public void Producer_Publish_after_stop_should_not_touch_disposed_channel()
        {
            var (producer, modelMock, _) = CreateProducer();
            producer.Start();
            producer.Stop();

            var message = new Message(MessageLabel.From("command.test"), new Dictionary<string, object>(), new { Id = 1 });

            Action act = () => producer.Publish(message);
            act.Should().Throw<InvalidOperationException>();

            modelMock.Verify(
                m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()),
                Times.Never);
        }

        [Test]
        public void Producer_Stop_should_be_idempotent_and_raise_Stopped_once()
        {
            var (producer, _, _) = CreateProducer();
            var stoppedCount = 0;
            producer.Stopped += (_, _) => stoppedCount++;

            producer.Start();
            producer.Stop();
            producer.Stop();

            stoppedCount.Should().Be(1);
        }

        [Test]
        public void FaultTolerantProducer_should_not_retry_when_disposed_during_send()
        {
            FaultTolerantProducer ftProducer = null;
            var calls = 0;

            var innerProducer = new Mock<IProducer>();
            innerProducer.SetupGet(p => p.BrokerUrl).Returns("amqp://localhost");
            innerProducer
                .Setup(p => p.Publish(It.IsAny<IMessage>()))
                .Callback(() =>
                {
                    calls++;
                    ftProducer.Dispose();
                    throw new ObjectDisposedException("Producer");
                });

            var selector = new Mock<IProducerSelector>();
            selector.Setup(s => s.Next()).Returns(innerProducer.Object);

            ftProducer = new FaultTolerantProducer(selector.Object, maxAttempts: 7, maxRetryDelay: 0, inactivityResetDelay: 0);

            var exchange = new MessageExchange(new Message(MessageLabel.From("command.test"), new { }));

            Action act = () => ftProducer.Send(exchange, null);
            act.Should().Throw<ObjectDisposedException>();
            calls.Should().Be(1);
        }

        [Test]
        public void RabbitDelivery_Accept_and_Reject_should_skip_when_channel_is_closed()
        {
            var busContext = new Mock<IBusContext>();
            busContext.SetupGet(b => b.MessageLabelHandler).Returns(new DefaultRabbitMessageLabelHandler());

            var modelMock = new Mock<IModel>();
            modelMock.SetupGet(m => m.IsOpen).Returns(false);

            var channel = new RabbitChannel(Guid.NewGuid(), modelMock.Object, busContext.Object, "amqp://localhost", "key");
            var props = new Mock<IBasicProperties>();
            props.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>());

            var delivery = new RabbitDelivery(
                busContext.Object,
                channel,
                new BasicDeliverEventArgs
                {
                    DeliveryTag = 42UL,
                    BasicProperties = props.Object
                },
                requiresAccept: true);

            delivery.Accept();
            delivery.Reject(true);

            modelMock.Verify(m => m.BasicAck(It.IsAny<ulong>(), It.IsAny<bool>()), Times.Never);
            modelMock.Verify(m => m.BasicNack(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>()), Times.Never);
        }
    }
}
