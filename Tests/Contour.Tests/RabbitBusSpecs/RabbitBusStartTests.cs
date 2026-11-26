using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Contour.Receiving.Consumers;
using Contour.Tests.Mocks.RabbitMq;
using Contour.Tests.TestConfiguration;
using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Internal;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.RabbitBusSpecs;

[TestFixture]
public class RabbitBusStartTests
{
    private RabbitBusConfigurator configurator;
    private TestDependencyResolver dependencyResolver;
    private MockRabbitConnectionPool connectionPool;
    private RabbitBus bus;
    private readonly List<BusLifecycleEventType> changedStates = [];

    [SetUp]
    public void SetUp()
    {
        this.connectionPool = new MockRabbitConnectionPool();
        this.configurator = new RabbitBusConfigurator
        {
            ConnectionPool = this.connectionPool
        };
        this.dependencyResolver = new TestDependencyResolver();
        var endpoint = TestEndpoint.Default;
        this.dependencyResolver.Register<IConsumer<TestIncomingMessage>>("TestConsumer", new TestIncomingMessageConsumer());
        this.configurator.Configure(endpoint, this.dependencyResolver.Resolve);
        this.configurator.Validate();
        this.bus = (RabbitBus)this.configurator.Create();
        this.connectionPool.Bus = this.bus;
        this.bus.Starting += _ => this.changedStates.Add(BusLifecycleEventType.Starting);
        this.bus.Started += _ => this.changedStates.Add(BusLifecycleEventType.Started);
        this.bus.Stopping += _ => this.changedStates.Add(BusLifecycleEventType.Stopping);
        this.bus.Stopped += _ => this.changedStates.Add(BusLifecycleEventType.Stopped);
    }

    [TearDown]
    public async Task TearDown()
    {
        if (this.bus != null)
        {
            await this.bus.DisposeAsync();
        }

        if (this.connectionPool != null)
        {
            await this.connectionPool.DisposeAsync();
        }

        this.changedStates.Clear();
    }

    [Test]
    public async Task Start_PreparedState_StartedOk()
    {
        await this.bus.Prepare();
        this.bus.State.Should().Be(BusState.Configured);

        await this.bus.Start();

        this.bus.State.Should().Be(BusState.Working);
        foreach (var sender in this.bus.Senders.Cast<RabbitSender>())
        {
            sender.IsHealthy.Should().BeTrue();
        }

        foreach (var receiver in this.bus.Receivers.Cast<RabbitReceiver>())
        {
            receiver.IsHealthy.Should().BeTrue();
        }

        this.changedStates.Should().BeEquivalentTo([BusLifecycleEventType.Starting, BusLifecycleEventType.Started]);
    }

    [Test]
    public async Task Start_FromNewState_PreparesAndStartsBus()
    {
        this.bus.State.Should().Be(BusState.NotInitialized);

        await this.bus.Start();

        this.bus.State.Should().Be(BusState.Working);
        foreach (var sender in this.bus.Senders.Cast<RabbitSender>())
        {
            sender.IsHealthy.Should().BeTrue();
        }

        foreach (var receiver in this.bus.Receivers.Cast<RabbitReceiver>())
        {
            receiver.IsHealthy.Should().BeTrue();
        }

        this.changedStates.Should().BeEquivalentTo([BusLifecycleEventType.Starting, BusLifecycleEventType.Started]);
    }

    [Test]
    public async Task Start_AlreadyStarted_DoesNothing()
    {
        await this.bus.Start();
        this.bus.State.Should().Be(BusState.Working);

        await this.bus.Start(); // second start

        this.bus.State.Should().Be(BusState.Working);
        foreach (var sender in this.bus.Senders.Cast<RabbitSender>())
        {
            sender.IsHealthy.Should().BeTrue();
        }

        foreach (var receiver in this.bus.Receivers.Cast<RabbitReceiver>())
        {
            receiver.IsHealthy.Should().BeTrue();
        }

        this.changedStates.Should().BeEquivalentTo([BusLifecycleEventType.Starting, BusLifecycleEventType.Started]);
    }

    [Test]
    public async Task Start_CancellationRequested_Throws()
    {
        var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(10));
        this.connectionPool.Delay = TimeSpan.FromSeconds(1);

        await this.bus.Invoking(b => b.Start(cts.Token))
            .Should()
            .ThrowAsync<OperationCanceledException>();

        this.changedStates.Should().BeEmpty();
    }

    [Test]
    public async Task Start_StopCalledWhilePreparing_Throws()
    {
        this.connectionPool.Delay = TimeSpan.FromSeconds(1);

        var t = this.bus.Invoking(b => b.Start()).Should()
            .ThrowAsync<OperationCanceledException>();
        await Task.Delay(10);
        await this.bus.Stop();

        this.changedStates.Should().BeEmpty();
        await t;
    }
}
