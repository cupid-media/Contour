using System.Threading;
using System.Threading.Tasks;
using Contour.Tests.Mocks.Kafka;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

/// <summary>
///   Трекер компонентов общий для обоих транспортов, поэтому и проверяется отдельно от них.
/// </summary>
[TestFixture]
[Category("Unit")]
public class BusComponentTrackerTests
{
    [Test]
    public async Task Receivers_are_started_and_stopped_as_a_group()
    {
        var tracker = new BusComponentTracker();
        var first = new FakeKafkaReceiver("first");
        var second = new FakeKafkaReceiver("second");

        tracker.Register(first);
        tracker.Register(second);

        await tracker.StartAllReceivers(CancellationToken.None);
        first.IsStarted.Should().BeTrue();
        second.IsStarted.Should().BeTrue();

        await tracker.StopAllReceivers(CancellationToken.None);
        first.IsStarted.Should().BeFalse();
        second.IsStarted.Should().BeFalse();
    }

    [Test]
    public void CheckHealth_reflects_component_health()
    {
        var tracker = new BusComponentTracker();
        var healthy = new FakeKafkaBusComponent();
        var sick = new FakeKafkaBusComponent { IsHealthy = false };

        tracker.Register(healthy);
        tracker.CheckHealth().Should().BeTrue();

        tracker.Register(sick);
        tracker.CheckHealth().Should().BeFalse();
    }

    [Test]
    public async Task UnregisterAll_disposes_and_clears_components()
    {
        var tracker = new BusComponentTracker();
        var component = new FakeKafkaBusComponent();
        tracker.Register(component);

        await tracker.UnregisterAll();

        component.IsDisposed.Should().BeTrue();
        tracker.AllOf<FakeKafkaBusComponent>().Should().BeEmpty();
    }

    [Test]
    public void AllOf_filters_components_by_type()
    {
        var tracker = new BusComponentTracker();
        tracker.Register(new FakeKafkaBusComponent());
        tracker.Register(new FakeKafkaReceiver());

        tracker.AllOf<FakeKafkaReceiver>().Should().ContainSingle();
        tracker.AllOf<FakeKafkaBusComponent>().Should().ContainSingle();
    }
}
