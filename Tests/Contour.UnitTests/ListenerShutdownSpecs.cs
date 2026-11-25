using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Serialization;
using Contour.Transport.RabbitMQ;
using Contour.Transport.RabbitMQ.Internal;
using Contour.Validation;
using FluentAssertions;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;

namespace Contour.UnitTests
{
    [TestFixture]
    public class ListenerShutdownSpecs
    {
        private static (Listener listener, Mock<IModel> modelMock, ManualResetEventSlim consumingStarted, Action deliverAction)
            CreateListenerWithAsyncConsumer(
                WaitHandle whenReady,
                IAsyncConsumerOf<TestPayload> consumer,
                Mock<IRabbitConnection> connectionMock = null)
        {
            var payloadConverter = new Mock<IPayloadConverter>();
            payloadConverter
                .Setup(c => c.ToObject(It.IsAny<byte[]>(), typeof(TestPayload)))
                .Returns(new TestPayload { Value = "ok" });
            payloadConverter
                .Setup(c => c.FromObject(It.IsAny<object>()))
                .Returns(Encoding.UTF8.GetBytes("{}"));
            payloadConverter.SetupGet(c => c.ContentType).Returns("application/json");

            var labelHandler = new DefaultRabbitMessageLabelHandler();

            var busContext = new Mock<IBusContext>();
            busContext.SetupGet(b => b.Endpoint).Returns(new Endpoint("test-endpoint"));
            busContext.SetupGet(b => b.WhenReady).Returns(whenReady);
            busContext.SetupGet(b => b.MessageLabelHandler).Returns(labelHandler);
            busContext.SetupGet(b => b.PayloadConverter).Returns(payloadConverter.Object);

            var modelOpen = true;
            IBasicConsumer capturedConsumer = null;
            var consumingStarted = new ManualResetEventSlim(false);

            var propsMock = new Mock<IBasicProperties>();
            propsMock.SetupAllProperties();
            propsMock.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>());

            var modelMock = new Mock<IModel>();
            modelMock.SetupGet(m => m.IsOpen).Returns(() => modelOpen);
            modelMock.Setup(m => m.CreateBasicProperties()).Returns(propsMock.Object);
            modelMock
                .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
                .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((q, autoAck, tag, noLocal, excl, args, basicConsumer) =>
                {
                    capturedConsumer = basicConsumer;
                    consumingStarted.Set();
                })
                .Returns("consumer-tag-1");
            modelMock
                .Setup(m => m.Abort())
                .Callback(() => modelOpen = false);
            modelMock
                .Setup(m => m.Dispose())
                .Callback(() => modelOpen = false);

            connectionMock ??= new Mock<IRabbitConnection>();
            connectionMock.SetupGet(c => c.ConnectionString).Returns("amqp://guest:guest@localhost:5672/vhost");
            connectionMock.SetupGet(c => c.ConnectionKey).Returns("127.0.0.1:5672:vhost");
            connectionMock
                .Setup(c => c.OpenChannel(It.IsAny<CancellationToken>()))
                .Returns(() => new RabbitChannel(Guid.NewGuid(), modelMock.Object, busContext.Object, "amqp://guest:guest@localhost:5672/vhost", "127.0.0.1:5672:vhost"));

            var listeningSource = new Mock<IListeningSource>();
            listeningSource.SetupGet(s => s.Address).Returns("test.queue");

            var endpoint = new Mock<ISubscriptionEndpoint>();
            endpoint.SetupGet(e => e.ListeningSource).Returns(listeningSource.Object);

            var options = new RabbitReceiverOptions
            {
                ParallelismLevel = 1,
                AcceptIsRequired = true,
                IncomingMessageHeaderStorage = new MessageHeaderStorage(new List<string>()),
                FailedDeliveryStrategy = new LambdaFailedDeliveryStrategy(_ => { }),
                UnhandledDeliveryStrategy = new LambdaUnhandledDeliveryStrategy(_ => { })
            };

            var listener = new Listener(
                busContext.Object,
                connectionMock.Object,
                endpoint.Object,
                options,
                new MessageValidatorRegistry());

            var label = MessageLabel.From("query.test");
            listener.RegisterConsumer(label, consumer, null);

            Action deliver = () =>
            {
                var deliverProps = new Mock<IBasicProperties>();
                deliverProps.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>
                {
                    { Headers.MessageLabel, Encoding.UTF8.GetBytes("query.test") }
                });
                deliverProps.SetupGet(p => p.CorrelationId).Returns((string)null);
                deliverProps.SetupGet(p => p.ReplyTo).Returns((string)null);

                capturedConsumer.HandleBasicDeliver(
                    "consumer-tag-1",
                    1UL,
                    false,
                    "query.test",
                    string.Empty,
                    deliverProps.Object,
                    Encoding.UTF8.GetBytes("{}"));
            };

            return (listener, modelMock, consumingStarted, deliver);
        }

        [Test]
        public void StopConsuming_should_wait_for_in_flight_async_consumer_before_disposing_channel()
        {
            var readyEvent = new ManualResetEvent(true);
            var handlerEntered = new ManualResetEventSlim(false);
            var releaseHandler = new ManualResetEventSlim(false);
            var ackedWhileChannelOpen = false;
            var channelAbortedBeforeHandlerFinished = false;

            var modelEvents = new List<string>();
            var consumerMock = new Mock<IAsyncConsumerOf<TestPayload>>();
            Mock<IModel> modelMockRef = null;

            consumerMock
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestPayload>>()))
                .Returns<IConsumingContext<TestPayload>>(async ctx =>
                {
                    handlerEntered.Set();
                    await Task.Run(() => releaseHandler.Wait(TimeSpan.FromSeconds(5)));

                    if (!modelMockRef.Object.IsOpen)
                    {
                        channelAbortedBeforeHandlerFinished = true;
                    }

                    ctx.Accept();
                    ackedWhileChannelOpen = modelMockRef.Object.IsOpen;
                    modelEvents.Add("acked");
                });

            var (listener, modelMock, consumingStarted, deliver) = CreateListenerWithAsyncConsumer(readyEvent, consumerMock.Object);
            modelMockRef = modelMock;

            modelMock
                .Setup(m => m.BasicAck(It.IsAny<ulong>(), It.IsAny<bool>()))
                .Callback(() => modelEvents.Add("BasicAck"));
            modelMock
                .Setup(m => m.Abort())
                .Callback(() => modelEvents.Add("Abort"));

            listener.StartConsuming();
            consumingStarted.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            deliver();
            handlerEntered.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            var stopTask = Task.Run(() => listener.StopConsuming());

            // Give StopConsuming time to reach worker.Wait(); channel must NOT be aborted yet
            Thread.Sleep(150);
            modelEvents.Should().NotContain("Abort");

            releaseHandler.Set();
            stopTask.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            channelAbortedBeforeHandlerFinished.Should().BeFalse();
            ackedWhileChannelOpen.Should().BeTrue();
            modelEvents.Should().ContainInOrder("BasicAck", "acked", "Abort");
        }

        [Test]
        public void StopConsuming_should_not_block_for_60s_when_bus_never_becomes_ready()
        {
            var neverReady = new ManualResetEvent(false);
            var openChannelCalled = new ManualResetEventSlim(false);
            var connectionMock = new Mock<IRabbitConnection>();

            var consumerMock = new Mock<IAsyncConsumerOf<TestPayload>>();
            var (listener, _, _, _) = CreateListenerWithAsyncConsumer(neverReady, consumerMock.Object, connectionMock);

            connectionMock
                .Setup(c => c.OpenChannel(It.IsAny<CancellationToken>()))
                .Callback(() => openChannelCalled.Set())
                .Returns(() => new RabbitChannel(Guid.NewGuid(), new Mock<IModel>().Object, new Mock<IBusContext>().Object, "amqp://localhost", "key"));

            listener.StartConsuming();
            openChannelCalled.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            var sw = Stopwatch.StartNew();
            listener.StopConsuming();
            sw.Stop();

            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2));
        }

        [Test]
        public void OnChannelShutdown_should_not_deadlock_dispatch_thread_while_async_consumer_awaits_continuation()
        {
            var readyEvent = new ManualResetEvent(true);
            var handlerEntered = new ManualResetEventSlim(false);
            var continuationGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var consumerMock = new Mock<IAsyncConsumerOf<TestPayload>>();
            consumerMock
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestPayload>>()))
                .Returns<IConsumingContext<TestPayload>>(async _ =>
                {
                    handlerEntered.Set();
                    await continuationGate.Task;
                });

            var (listener, modelMock, consumingStarted, deliver) = CreateListenerWithAsyncConsumer(readyEvent, consumerMock.Object);

            listener.StartConsuming();
            consumingStarted.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            deliver();
            handlerEntered.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            // Simulate RabbitMQ I/O loop thread: it fires ModelShutdown and can only complete the
            // in-flight RPC/continuation AFTER ModelShutdown returns on that same thread.
            var ioThreadCompleted = Task.Run(() =>
            {
                modelMock.Raise(
                    m => m.ModelShutdown += null,
                    new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "CONNECTION_FORCED"));

                continuationGate.TrySetResult(true);
            });

            ioThreadCompleted.Wait(TimeSpan.FromSeconds(1)).Should().BeTrue(
                "OnChannelShutdown must not block the AMQP I/O thread waiting for an in-flight async worker");

            listener.StopConsuming();
        }

        [Test]
        public void StopConsuming_should_bound_wait_time_when_worker_is_hung_and_still_close_channel()
        {
            var readyEvent = new ManualResetEvent(true);
            var handlerEntered = new ManualResetEventSlim(false);
            var neverCompletes = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            var consumerMock = new Mock<IAsyncConsumerOf<TestPayload>>();
            consumerMock
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestPayload>>()))
                .Returns<IConsumingContext<TestPayload>>(async _ =>
                {
                    handlerEntered.Set();
                    await neverCompletes.Task;
                });

            var (listener, modelMock, consumingStarted, deliver) = CreateListenerWithAsyncConsumer(readyEvent, consumerMock.Object);
            listener.WorkerDrainTimeout.Should().BeLessOrEqualTo(TimeSpan.FromSeconds(30),
                "Listener must have a finite default WorkerDrainTimeout so hung handlers cannot block pod shutdown forever");
            listener.WorkerDrainTimeout = TimeSpan.FromMilliseconds(150);

            listener.StartConsuming();
            consumingStarted.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            deliver();
            handlerEntered.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            var stopTask = Task.Run(() => listener.StopConsuming());
            var completedWithinBound = stopTask.Wait(TimeSpan.FromSeconds(1));

            neverCompletes.TrySetResult(true);

            completedWithinBound.Should().BeTrue("StopConsuming must respect WorkerDrainTimeout when a handler hangs");
            modelMock.Object.IsOpen.Should().BeFalse("Channel must still be closed after drain timeout expires");
        }

        public class TestPayload
        {
            public string Value { get; set; }
        }
    }
}
