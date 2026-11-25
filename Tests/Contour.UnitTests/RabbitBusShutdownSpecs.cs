using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Contour.Configuration;
using Contour.Receiving;
using Contour.Receiving.Consumers;
using Contour.Serialization;
using Contour.Transport.RabbitMQ;
using Contour.Transport.RabbitMQ.Internal;
using FluentAssertions;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace Contour.UnitTests
{
    [TestFixture]
    public class RabbitBusShutdownSpecs
    {
        private class JsonPayloadConverter : IPayloadConverter
        {
            public string ContentType => "application/json";

            public byte[] FromObject(object payload)
            {
                return JsonSerializer.SerializeToUtf8Bytes(payload);
            }

            public object ToObject(byte[] payload, Type targetType)
            {
                return JsonSerializer.Deserialize(payload, targetType);
            }
        }

        public class TestRequest
        {
            public int UserId { get; set; }
        }

        public class TestResponse
        {
            public string Status { get; set; }
        }

        private sealed class FakeBusHarness
        {
            public Mock<IConnectionPool<IRabbitConnection>> ConnectionPoolMock { get; } = new Mock<IConnectionPool<IRabbitConnection>>();
            public ConcurrentDictionary<string, IBasicConsumer> ConsumersByQueue { get; } = new ConcurrentDictionary<string, IBasicConsumer>();
            public List<string> Events { get; } = new List<string>();
            public object EventsLock { get; } = new object();
            public bool PoolDropped { get; private set; }
            public int AlreadyClosedErrors { get; private set; }

            public FakeBusHarness()
            {
                this.ConnectionPoolMock
                    .Setup(p => p.Get(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                    .Returns<string, bool, CancellationToken>((url, _, _) =>
                    {
                        lock (this.EventsLock)
                        {
                            this.PoolDropped = false;
                        }

                        return this.CreateConnection(url);
                    });

                this.ConnectionPoolMock
                    .Setup(p => p.Drop())
                    .Callback(() =>
                    {
                        lock (this.EventsLock)
                        {
                            this.PoolDropped = true;
                            this.Events.Add("connectionPool.Drop");
                        }
                    });
            }

            public Action<string, string, IBasicProperties, ReadOnlyMemory<byte>> OnBasicPublish { get; set; }

            private IRabbitConnection CreateConnection(string url)
            {
                var connMock = new Mock<IRabbitConnection>();
                connMock.SetupGet(c => c.Id).Returns(Guid.NewGuid());
                connMock.SetupGet(c => c.ConnectionString).Returns(url);
                connMock.SetupGet(c => c.ConnectionKey).Returns("127.0.0.1:5672:users.tags");
                connMock.SetupGet(c => c.IsBlocked).Returns(false);

                connMock
                    .Setup(c => c.OpenChannel(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        var modelOpen = true;
                        var modelMock = new Mock<IModel>();
                        modelMock.SetupGet(m => m.IsOpen).Returns(() => modelOpen && !this.PoolDropped);
                        modelMock.Setup(m => m.QueueDeclare(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>()))
                            .Returns<string, bool, bool, bool, IDictionary<string, object>>((q, _, _, _, _) =>
                                new QueueDeclareOk(string.IsNullOrEmpty(q) ? "amq.gen-callback" : q, 0, 0));

                        modelMock.Setup(m => m.CreateBasicProperties()).Returns(() =>
                        {
                            var props = new Mock<IBasicProperties>();
                            props.SetupAllProperties();
                            props.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>());
                            return props.Object;
                        });

                        modelMock
                            .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
                            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((queue, _, _, _, _, _, consumer) =>
                            {
                                this.ConsumersByQueue[queue] = consumer;
                            })
                            .Returns<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((queue, _, _, _, _, _, _) => $"tag-{queue}");

                        modelMock
                            .Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<IBasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>()))
                            .Callback<string, string, bool, IBasicProperties, ReadOnlyMemory<byte>>((exchange, routingKey, _, props, body) =>
                            {
                                if (!modelOpen || this.PoolDropped)
                                {
                                    lock (this.EventsLock)
                                    {
                                        this.AlreadyClosedErrors++;
                                    }

                                    throw new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Goodbye"));
                                }

                                lock (this.EventsLock)
                                {
                                    this.Events.Add($"BasicPublish:{exchange}");
                                }

                                this.OnBasicPublish?.Invoke(exchange, routingKey, props, body);
                            });

                        modelMock
                            .Setup(m => m.BasicAck(It.IsAny<ulong>(), It.IsAny<bool>()))
                            .Callback<ulong, bool>((_, _) =>
                            {
                                if (!modelOpen || this.PoolDropped)
                                {
                                    lock (this.EventsLock)
                                    {
                                        this.AlreadyClosedErrors++;
                                    }

                                    throw new AlreadyClosedException(new ShutdownEventArgs(ShutdownInitiator.Application, 200, "Goodbye"));
                                }

                                lock (this.EventsLock)
                                {
                                    this.Events.Add("BasicAck");
                                }
                            });

                        modelMock.Setup(m => m.Abort()).Callback(() => modelOpen = false);
                        modelMock.Setup(m => m.Dispose()).Callback(() => modelOpen = false);

                        return new RabbitChannel(
                            connMock.Object.Id,
                            modelMock.Object,
                            this.Bus,
                            url,
                            "127.0.0.1:5672:users.tags");
                    });

                return connMock.Object;
            }

            public RabbitBus Bus { get; set; }
        }

        [Test]
        public void Shutdown_should_drain_in_flight_async_consumer_using_request_reply_before_dropping_connections()
        {
            var harness = new FakeBusHarness();
            var handlerStarted = new ManualResetEventSlim(false);
            var releaseBeforeOutgoingRequest = new ManualResetEventSlim(false);
            var consumerCompleted = new ManualResetEventSlim(false);
            Exception consumerException = null;

            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());

            RabbitBus bus = null;

            config.Route("query.users.tags.adapter.get")
                .WithDefaultCallbackEndpoint();

            var consumerMock = new Mock<IAsyncConsumerOf<TestRequest>>();
            consumerMock
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestRequest>>()))
                .Returns<IConsumingContext<TestRequest>>(async ctx =>
                {
                    try
                    {
                        handlerStarted.Set();
                        await Task.Run(() => releaseBeforeOutgoingRequest.Wait(TimeSpan.FromSeconds(5)));

                        var adapterReply = await bus.RequestAsync<TestRequest, TestResponse>(
                            "query.users.tags.adapter.get",
                            new TestRequest { UserId = ctx.Message.Payload.UserId });

                        ctx.Reply(adapterReply);
                        ctx.Accept();
                    }
                    catch (Exception ex)
                    {
                        consumerException = ex;
                        throw;
                    }
                    finally
                    {
                        consumerCompleted.Set();
                    }
                });

            config.On<TestRequest>("query.users.tags.get")
                .RequiresAccept()
                .ReactWith(consumerMock.Object);

            bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;

            // Simulate adapter responding on the callback queue whenever query.users.tags.adapter.get is published
            harness.OnBasicPublish = (exchange, _, props, _) =>
            {
                if (exchange == "query.users.tags.adapter.get")
                {
                    Task.Run(() =>
                    {
                        foreach (var kvp in harness.ConsumersByQueue)
                        {
                            if (kvp.Key.StartsWith("users-tags-manager.replies-", StringComparison.Ordinal))
                            {
                                var replyProps = new Mock<IBasicProperties>();
                                replyProps.SetupGet(p => p.CorrelationId).Returns(props.CorrelationId);
                                replyProps.SetupGet(p => p.ReplyTo).Returns((string)null);
                                replyProps.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>());

                                var replyBytes = JsonSerializer.SerializeToUtf8Bytes(new TestResponse { Status = "ok" });
                                kvp.Value.HandleBasicDeliver(
                                    "tag-callback",
                                    1UL,
                                    false,
                                    string.Empty,
                                    kvp.Key,
                                    replyProps.Object,
                                    replyBytes);
                                break;
                            }
                        }
                    });
                }
            };

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.IsReady.Should().BeTrue();

            // Wait until both incoming and callback queue consumers are registered
            SpinWait.SpinUntil(
                    () => harness.ConsumersByQueue.ContainsKey("users-tags-manager.query.users.tags.get")
                          && System.Linq.Enumerable.Any(harness.ConsumersByQueue.Keys, k => k.StartsWith("users-tags-manager.replies-", StringComparison.Ordinal)),
                    TimeSpan.FromSeconds(5))
                .Should().BeTrue($"Registered consumers: [{string.Join(", ", harness.ConsumersByQueue.Keys)}]");

            // Deliver incoming request with ReplyTo and CorrelationId
            var incomingProps = new Mock<IBasicProperties>();
            incomingProps.SetupGet(p => p.CorrelationId).Returns("corr-123");
            incomingProps.SetupGet(p => p.ReplyTo).Returns("direct://reply.exchange/reply.key");
            incomingProps.SetupGet(p => p.ReplyToAddress).Returns(new PublicationAddress("direct", "reply.exchange", "reply.key"));
            incomingProps.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>
            {
                { Headers.MessageLabel, Encoding.UTF8.GetBytes("query.users.tags.get") }
            });

            harness.ConsumersByQueue["users-tags-manager.query.users.tags.get"].HandleBasicDeliver(
                "tag-incoming",
                10UL,
                false,
                "query.users.tags.get",
                string.Empty,
                incomingProps.Object,
                JsonSerializer.SerializeToUtf8Bytes(new TestRequest { UserId = 42 }));

            handlerStarted.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            // Trigger graceful shutdown (simulating k8s SIGTERM while handler is in-flight)
            var shutdownTask = Task.Run(() => bus.Shutdown());

            // Give Shutdown time to start stopping receivers; connectionPool.Drop must NOT have happened yet
            Thread.Sleep(150);
            harness.PoolDropped.Should().BeFalse();

            // Let the in-flight handler execute RequestAsync -> Reply -> Accept
            releaseBeforeOutgoingRequest.Set();

            consumerCompleted.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue(
                $"Events=[{string.Join(", ", harness.Events)}], Consumers=[{string.Join(", ", harness.ConsumersByQueue.Keys)}], Exception={consumerException}");
            shutdownTask.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            consumerException.Should().BeNull();
            harness.AlreadyClosedErrors.Should().Be(0);
            harness.PoolDropped.Should().BeTrue();
            bus.IsStarted.Should().BeFalse();
            bus.IsReady.Should().BeFalse();

            harness.Events.Should().ContainInOrder(
                "BasicPublish:query.users.tags.adapter.get",
                "BasicPublish:reply.exchange",
                "BasicAck",
                "connectionPool.Drop");
        }

        [Test]
        public void Emit_after_Shutdown_should_throw_BusNotReadyException()
        {
            var harness = new FakeBusHarness();
            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());
            config.Route("event.test");

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.Shutdown();

            Action act = () => bus.Emit("event.test", new TestRequest { UserId = 1 });
            act.Should().Throw<BusNotReadyException>();

            bus.IsStarted.Should().BeFalse();
            bus.IsReady.Should().BeFalse();
        }

        [Test]
        public void Emit_during_active_Shutdown_after_ready_reset_should_fail_fast_without_5s_wait()
        {
            var harness = new FakeBusHarness();
            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());
            config.Route("event.test");

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.Stop();
            bus.IsShuttingDown = true;

            var sw = Stopwatch.StartNew();
            Action act = () => bus.Emit("event.test", new TestRequest { UserId = 1 });
            act.Should().Throw<BusNotReadyException>();
            sw.Stop();

            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1));
        }

        [Test]
        public void Start_after_Stop_and_after_Shutdown_should_restart_cleanly()
        {
            var harness = new FakeBusHarness();
            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());
            config.Route("event.test");

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.IsReady.Should().BeTrue();
            bus.IsStarted.Should().BeTrue();

            bus.Stop();
            bus.IsReady.Should().BeFalse();
            bus.IsStarted.Should().BeFalse();

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.IsReady.Should().BeTrue();
            bus.IsStarted.Should().BeTrue();
            FluentActions.Invoking(() => bus.Emit("event.test", new TestRequest { UserId = 1 }).Wait(TimeSpan.FromSeconds(5))).Should().NotThrow();

            bus.Shutdown();
            bus.IsReady.Should().BeFalse();
            bus.IsStarted.Should().BeFalse();

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.IsReady.Should().BeTrue();
            bus.IsStarted.Should().BeTrue();
            FluentActions.Invoking(() => bus.Emit("event.test", new TestRequest { UserId = 2 }).Wait(TimeSpan.FromSeconds(5))).Should().NotThrow();
        }

        [Test]
        public void RabbitSender_should_not_reenlist_producer_when_channel_shuts_down_during_bus_shutdown()
        {
            var openChannelCount = 0;
            var modelMocks = new List<Mock<IModel>>();

            var connPoolMock = new Mock<IConnectionPool<IRabbitConnection>>();
            var connMock = new Mock<IRabbitConnection>();
            connMock.SetupGet(c => c.Id).Returns(Guid.NewGuid());
            connMock.SetupGet(c => c.ConnectionString).Returns("amqp://guest:guest@localhost:5672/users.tags");
            connMock.SetupGet(c => c.ConnectionKey).Returns("127.0.0.1:5672:users.tags");
            connMock.SetupGet(c => c.IsBlocked).Returns(false);

            var busConfig = new BusConfiguration();
            busConfig.UseRabbitMq();
            busConfig.SetEndpoint("users-tags-manager");
            busConfig.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            busConfig.UsePayloadConverter(new JsonPayloadConverter());
            busConfig.Route("event.test");

            var bus = new RabbitBus(busConfig, connPoolMock.Object);

            connMock
                .Setup(c => c.OpenChannel(It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    openChannelCount++;
                    var isOpen = true;
                    var modelMock = new Mock<IModel>();
                    modelMock.SetupGet(m => m.IsOpen).Returns(() => isOpen);
                    modelMock.Setup(m => m.Abort()).Callback(() => isOpen = false);
                    modelMock.Setup(m => m.Dispose()).Callback(() => isOpen = false);
                    modelMocks.Add(modelMock);
                    return new RabbitChannel(connMock.Object.Id, modelMock.Object, bus, "amqp://guest:guest@localhost:5672/users.tags", "127.0.0.1:5672:users.tags");
                });

            connPoolMock
                .Setup(p => p.Get(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns(connMock.Object);

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            var initialOpenCount = openChannelCount;
            modelMocks.Should().NotBeEmpty();

            bus.IsShuttingDown = true;

            // Simulate non-application channel shutdown (e.g. broker/network drop while bus is shutting down)
            var task = Task.Run(() =>
            {
                modelMocks[initialOpenCount - 1].Raise(
                    m => m.ModelShutdown += null,
                    new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "CONNECTION_FORCED"));
            });

            task.Wait(TimeSpan.FromSeconds(2)).Should().BeTrue();
            openChannelCount.Should().Be(initialOpenCount);

            bus.IsShuttingDown = false;
            bus.Shutdown();
        }
    }
}
