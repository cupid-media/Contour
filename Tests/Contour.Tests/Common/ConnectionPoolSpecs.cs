using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Moq;
using NUnit.Framework;

namespace Contour.Tests.Common;

/// <summary>
/// Connection pool specifications
/// </summary>
public class ConnectionPoolSpecs
{
    [Category("Unit")]
    public class when_declaring_connection_pool
    {
        [Test]
        public async Task should_dispose_connections_on_pool_disposal()
        {
            var conString = string.Empty;
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.IsActive).Returns(true);

            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() => connection.Object);

            var pool = new FakeConnectionPool(provider.Object);
            await pool.Get(conString, false, CancellationToken.None);
            await pool.DisposeAsync();

            connection.Verify(c => c.DisposeAsync());
        }

        [Test]
        public async Task should_throw_if_disposed_pool_is_accessed()
        {
            var conString = string.Empty;
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.IsActive).Returns(true);

            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() => connection.Object);

            var pool = new FakeConnectionPool(provider.Object);
            await pool.DisposeAsync();
            await pool.Invoking(p => p.Get(conString, false, CancellationToken.None))
                .Should()
                .ThrowAsync<Exception>();
        }

        [Test]
        public async Task should_close_and_dispose_all_connections_on_pool_drop()
        {
            var conString = string.Empty;
            var cons = new List<Mock<IConnection>>();
            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() =>
            {
                var con = new Mock<IConnection>();
                con.Setup(c => c.IsActive).Returns(true);
                cons.Add(con);
                return con.Object;
            });

            const int Count = 5;
            var pool = new FakeConnectionPool(provider.Object);

            var i = 0;
            while (i++ < Count)
            {
                await pool.Get(conString, false, CancellationToken.None);
            }

            await pool.Drop();

            while (cons.Any())
            {
                var con = cons.First();
                con.Verify(c => c.Close());
                con.Verify(c => c.DisposeAsync());

                cons.Remove(con);
            }
        }

        [Test]
        public async Task should_cancel_not_started_get_operation()
        {
            var conString = string.Empty;
            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() =>
            {
                var con = new Mock<IConnection>();
                con.Setup(c => c.IsActive).Returns(true);
                return con.Object;
            });

            var pool = new FakeConnectionPool(provider.Object);

            var source = new CancellationTokenSource();
            source.Cancel();

            await pool.Invoking(p => p.Get(conString, false, source.Token))
                .Should()
                .ThrowAsync<OperationCanceledException>();
        }

        [Test]
        public async Task should_create_and_use_connection_if_reusable_for_empty_pool_group()
        {
            var conString = string.Empty;
            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() =>
            {
                var con = new Mock<IConnection>();
                con.Setup(c => c.IsActive).Returns(true);
                return con.Object;
            });

            var pool = new FakeConnectionPool(provider.Object);
            var connection = await pool.Get(conString, true, CancellationToken.None);

            connection.Should().NotBeNull();
        }

        [Test]
        public async Task should_reuse_connections_in_group_if_reusable()
        {
            var conString = string.Empty;
            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() =>
            {
                var con = new Mock<IConnection>();
                con.Setup(c => c.IsActive).Returns(true);
                return con.Object;
            });

            var pool = new FakeConnectionPool(provider.Object);
            var c1 = await pool.Get(conString, true, CancellationToken.None);
            var c2 = await pool.Get(conString, true, CancellationToken.None);

            c1.Should().Be(c2);
        }

        [Test]
        public async Task should_not_reuse_connections_in_group_if_not_reusable()
        {
            var conString = string.Empty;
            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() =>
            {
                var con = new Mock<IConnection>();
                con.Setup(c => c.IsActive).Returns(true);
                return con.Object;
            });

            var pool = new FakeConnectionPool(provider.Object);
            var c1 = await pool.Get(conString, false, CancellationToken.None);
            var c2 = await pool.Get(conString, false, CancellationToken.None);

            c1.Should().NotBe(c2);
        }

        [Test]
        public async Task should_not_reuse_connections_marked_as_non_reusable()
        {
            var conString = string.Empty;
            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(conString)).Returns(() =>
            {
                var con = new Mock<IConnection>();
                con.Setup(c => c.IsActive).Returns(true);
                return con.Object;
            });

            var pool = new FakeConnectionPool(provider.Object);
            var c1 = await pool.Get(conString, false, CancellationToken.None);
            var c2 = await pool.Get(conString, true, CancellationToken.None);

            c1.Should().NotBe(c2);

            var c3 = await pool.Get(conString, true, CancellationToken.None);
            c1.Should().NotBe(c3);
            c2.Should().Be(c3);
        }

        [Test]
        public async Task should_not_reuse_connections_in_different_groups()
        {
            const string ConnectionString1 = "1";
            const string ConnectionString2 = "2";

            var provider = new Mock<IConnectionProvider<IConnection>>();
            provider.Setup(cp => cp.Create(It.IsAny<string>())).Returns(() =>
            {
                var con = new Mock<IConnection>();
                return con.Object;
            });

            var pool = new FakeConnectionPool(provider.Object);
            var c1 = await pool.Get(ConnectionString1, true, CancellationToken.None);
            var c2 = await pool.Get(ConnectionString2, true, CancellationToken.None);

            c1.Should().NotBe(c2);
        }
    }
}