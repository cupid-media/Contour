using Contour.Transport.RabbitMQ.Topology;
using FluentAssertions;
using NUnit.Framework;

namespace Contour.Tests.Common;

class QueueBuilderSpecs
{
    /// <summary>
    /// When using limit of queue
    /// </summary>
    [TestFixture]
    [Category("Unit")]
    public class WhenUsingLimitOfQueue
    {
        /// <summary>
        /// Then should setub limit
        /// </summary>
        [Test]
        public void ShouldSetupLimit()
        {
            QueueBuilder sut = new QueueBuilder("queue").WithLimit(1000);
            sut.Instance.Limit.Should().Be(1000, "Should be set correct limit of message amount.");
        }
    }
}