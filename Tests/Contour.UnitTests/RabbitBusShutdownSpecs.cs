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

                        var channelQueueName = (string)null;
                        modelMock
                            .Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<IBasicConsumer>()))
                            .Callback<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((queue, _, _, _, _, _, consumer) =>
                            {
                                channelQueueName = queue;
                                this.ConsumersByQueue[queue] = consumer;
                            })
                            .Returns<string, bool, string, bool, bool, IDictionary<string, object>, IBasicConsumer>((queue, _, _, _, _, _, _) => $"tag-{queue}");

                        Action closeChannel = () =>
                        {
                            if (modelOpen)
                            {
                                modelOpen = false;
                                if (channelQueueName != null)
                                {
                                    lock (this.EventsLock)
                                    {
                                        this.Events.Add($"ChannelClosed:{channelQueueName}");
                                    }
                                }
                            }
                        };

                        modelMock.Setup(m => m.Abort()).Callback(closeChannel);
                        modelMock.Setup(m => m.Dispose()).Callback(closeChannel);

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

        [Test]
        public void Bus_with_CallbackEndpoint_should_be_healthy_close_callback_channel_before_drop_and_not_leak_receivers_on_restart()
        {
            var harness = new FakeBusHarness();
            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());

            config.Route("query.users.tags.adapter.get")
                .WithDefaultCallbackEndpoint();

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.ComponentTracker.CheckHealth().Should().BeTrue(
                "a started bus with callback endpoints must report healthy components");

            var receiverCountAfterFirstStart = System.Linq.Enumerable.Count(bus.Receivers);

            bus.Stop();
            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            bus.ComponentTracker.CheckHealth().Should().BeTrue();

            var receiverCountAfterRestart = System.Linq.Enumerable.Count(bus.Receivers);
            receiverCountAfterRestart.Should().Be(
                receiverCountAfterFirstStart,
                "restarting the bus via Stop() -> Start() must not accumulate duplicate RabbitCallbackReceiver instances");

            lock (harness.EventsLock)
            {
                harness.Events.Clear();
            }

            bus.Shutdown();

            var callbackCloseIdx = harness.Events.FindIndex(e => e.StartsWith("ChannelClosed:users-tags-manager.replies-", StringComparison.Ordinal));
            var dropIdx = harness.Events.IndexOf("connectionPool.Drop");

            callbackCloseIdx.Should().BeGreaterOrEqualTo(0, $"Callback listener channel must be closed on Shutdown. Events: [{string.Join(", ", harness.Events)}]");
            dropIdx.Should().BeGreaterThan(callbackCloseIdx, $"Callback channel must close BEFORE connectionPool.Drop. Events: [{string.Join(", ", harness.Events)}]");
        }

        [Test]
        public void Stop_should_stop_all_receivers_in_parallel_so_second_queue_does_not_start_queued_messages_while_first_drains()
        {
            var harness = new FakeBusHarness();
            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());

            var q1FirstEntered = new ManualResetEventSlim(false);
            var q2FirstEntered = new ManualResetEventSlim(false);
            var q1ProcessedCount = 0;
            var q2ProcessedCount = 0;

            var q1Consumer = new Mock<IAsyncConsumerOf<TestRequest>>();
            q1Consumer
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestRequest>>()))
                .Returns<IConsumingContext<TestRequest>>(async ctx =>
                {
                    if (Interlocked.Increment(ref q1ProcessedCount) == 1)
                    {
                        q1FirstEntered.Set();
                    }

                    await Task.Delay(300);
                    ctx.Accept();
                });

            var q2Consumer = new Mock<IAsyncConsumerOf<TestRequest>>();
            q2Consumer
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestRequest>>()))
                .Returns<IConsumingContext<TestRequest>>(async ctx =>
                {
                    if (Interlocked.Increment(ref q2ProcessedCount) == 1)
                    {
                        q2FirstEntered.Set();
                    }

                    await Task.Delay(300);
                    ctx.Accept();
                });

            config.On<TestRequest>("command.test.q1").RequiresAccept().ReactWith(q1Consumer.Object);
            config.On<TestRequest>("command.test.q2").RequiresAccept().ReactWith(q2Consumer.Object);

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;
            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            SpinWait.SpinUntil(() => harness.ConsumersByQueue.Count == 2, TimeSpan.FromSeconds(5)).Should().BeTrue();

            void Deliver(string label, ulong tag, int id)
            {
                var props = new Mock<IBasicProperties>();
                props.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>
                {
                    { Headers.MessageLabel, Encoding.UTF8.GetBytes(label) }
                });

                harness.ConsumersByQueue[$"users-tags-manager.{label}"].HandleBasicDeliver(
                    $"tag-{label}",
                    tag,
                    false,
                    label,
                    string.Empty,
                    props.Object,
                    JsonSerializer.SerializeToUtf8Bytes(new TestRequest { UserId = id }));
            }

            // Deliver 2 messages to BOTH q1 and q2 (1 in-flight 300ms + 1 buffered in prefetch queue on each).
            // Regardless of enumeration order, sequential stopping stops one queue for 300ms while the other queue
            // finishes its 1st message and starts its 2nd buffered message!
            Deliver("command.test.q1", 1UL, 1);
            Deliver("command.test.q1", 2UL, 2);
            Deliver("command.test.q2", 1UL, 10);
            Deliver("command.test.q2", 2UL, 11);

            q1FirstEntered.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            q2FirstEntered.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            bus.Stop();

            q1ProcessedCount.Should().Be(1,
                "stopping receivers in parallel must immediately cancel q1 so its 2nd prefetched message is not started");
            q2ProcessedCount.Should().Be(1,
                "stopping receivers in parallel must immediately cancel q2 so its 2nd prefetched message is not started");
            harness.AlreadyClosedErrors.Should().Be(0);

            bus.Shutdown();
        }

        [Test]
        public void Shutdown_should_not_reset_IsConfigured_while_StopTask_is_still_draining_past_StopWaitTimeout()
        {
            var harness = new FakeBusHarness();
            var handlerStarted = new ManualResetEventSlim(false);
            bool? isConfiguredDuringOnStopped = null;

            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());

            var consumerMock = new Mock<IAsyncConsumerOf<TestRequest>>();
            consumerMock
                .Setup(c => c.HandleAsync(It.IsAny<IConsumingContext<TestRequest>>()))
                .Returns<IConsumingContext<TestRequest>>(async ctx =>
                {
                    handlerStarted.Set();
                    await Task.Delay(250);
                    ctx.Accept();
                });

            config.On<TestRequest>("command.slow")
                .RequiresAccept()
                .ReactWith(consumerMock.Object);

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            bus.StopWaitTimeout.Should().Be(TimeSpan.FromSeconds(5));
            bus.StopWaitTimeout = TimeSpan.FromMilliseconds(50);
            harness.Bus = bus;
            bus.Stopped += (b, _) =>
            {
                isConfiguredDuringOnStopped = ((AbstractBus)b).IsConfigured;
            };

            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();
            SpinWait.SpinUntil(() => harness.ConsumersByQueue.ContainsKey("users-tags-manager.command.slow"), TimeSpan.FromSeconds(5)).Should().BeTrue();

            var props = new Mock<IBasicProperties>();
            props.SetupGet(p => p.Headers).Returns(new Dictionary<string, object>
            {
                { Headers.MessageLabel, Encoding.UTF8.GetBytes("command.slow") }
            });

            harness.ConsumersByQueue["users-tags-manager.command.slow"].HandleBasicDeliver(
                "tag-slow",
                1UL,
                false,
                "command.slow",
                string.Empty,
                props.Object,
                JsonSerializer.SerializeToUtf8Bytes(new TestRequest { UserId = 99 }));

            handlerStarted.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            bus.Shutdown();

            isConfiguredDuringOnStopped.Should().BeTrue(
                "Shutdown() must wait for StopTask to finish completely before resetting IsConfigured");
            bus.IsConfigured.Should().BeFalse();
            harness.AlreadyClosedErrors.Should().Be(0);
        }

        [Test]
        public void Shutdown_should_cancel_pending_RequestAsync_expectations_when_sender_stops_instead_of_waiting_30s_timeout()
        {
            var harness = new FakeBusHarness();
            var config = new BusConfiguration();
            config.UseRabbitMq();
            config.SetEndpoint("users-tags-manager");
            config.SetConnectionString("amqp://guest:guest@localhost:5672/users.tags");
            config.UsePayloadConverter(new JsonPayloadConverter());

            config.Route("query.never.replies")
                .WithRequestTimeout(TimeSpan.FromSeconds(30))
                .WithDefaultCallbackEndpoint();

            var bus = new RabbitBus(config, harness.ConnectionPoolMock.Object);
            harness.Bus = bus;
            bus.Start().Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            var published = new ManualResetEventSlim(false);
            harness.OnBasicPublish = (exchange, _, _, _) =>
            {
                if (exchange == "query.never.replies")
                {
                    published.Set();
                }
            };

            var requestTask = bus.RequestAsync<TestRequest, TestResponse>("query.never.replies", new TestRequest { UserId = 7 });
            published.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue();

            var sw = Stopwatch.StartNew();
            bus.Shutdown();

            Action waitRequest = () => requestTask.GetAwaiter().GetResult();
            waitRequest.Should().Throw<OperationCanceledException>();
            sw.Stop();

            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2));
        }
    }
}
