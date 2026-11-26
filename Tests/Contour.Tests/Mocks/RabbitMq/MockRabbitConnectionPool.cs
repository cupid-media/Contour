using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Contour.Transport.RabbitMQ.Internal;

namespace Contour.Tests.Mocks.RabbitMq;

internal class MockRabbitConnectionPool : IConnectionPool<IRabbitConnection>
{
    private readonly ConcurrentDictionary<string, IRabbitConnection> connections;
    private readonly ConcurrentDictionary<string, IRabbitConnection> reusableConnections;
    private bool isDisposed;

    public TimeSpan? Delay { get; set; }

    public MockRabbitConnectionPool()
    {
        this.connections = new ConcurrentDictionary<string, IRabbitConnection>();
        this.reusableConnections = new ConcurrentDictionary<string, IRabbitConnection>();
    }

    public RabbitBus Bus { get; set; }

    public int Count => this.connections.Count;

    public async Task<IRabbitConnection> Get(string connectionString, bool reusable, CancellationToken token)
    {
        ThrowIfDisposed();
        token.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or empty.", nameof(connectionString));
        }

        if (Bus == null)
        {
            throw new InvalidOperationException("BusContext must be set before creating connections.");
        }

        IRabbitConnection connection;

        if (reusable)
        {
            // For reusable connections, return existing one if available
            connection = this.reusableConnections.GetOrAdd(connectionString, cs =>
            {
                var mockConnection = new MockRabbitConnection(cs, GetConnectionKey(cs), Bus);
                mockConnection.Open(token).Wait();
                this.connections.TryAdd($"reusable_{cs}_{mockConnection.Id}", mockConnection);
                return mockConnection;
            });
        }
        else
        {
            // For non-reusable connections, always create a new one
            connection = new MockRabbitConnection(connectionString, GetConnectionKey(connectionString), Bus);
            await connection.Open(token);
            this.connections.TryAdd($"single_{connectionString}_{connection.Id}", connection);
        }

        if (Delay.HasValue)
        {
            await Task.Delay(Delay.Value);
        }

        return connection;
    }

    public async Task Drop()
    {
        ThrowIfDisposed();

        // Close and dispose all connections
        var disposeTasks = new Task[this.connections.Count];
        var index = 0;

        foreach (var kvp in this.connections)
        {
            var connection = kvp.Value;
            disposeTasks[index++] = DisposeConnectionSafely(connection);
        }

        if (disposeTasks.Length > 0)
        {
            await Task.WhenAll(disposeTasks);
        }

        this.connections.Clear();
        this.reusableConnections.Clear();

        if (Delay.HasValue)
        {
            await Task.Delay(Delay.Value);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!this.isDisposed)
        {
            await Drop();
            this.isDisposed = true;
        }
    }

    private static string GetConnectionKey(string connectionString)
    {
        return $"mock_key_{connectionString.GetHashCode():X}";
    }

    private static async Task DisposeConnectionSafely(IRabbitConnection connection)
    {
        try
        {
            await connection.Close();
            await connection.DisposeAsync();
        }
        catch
        {
            // Ignore disposal exceptions in mock
        }
    }

    private void ThrowIfDisposed()
    {
        if (this.isDisposed)
        {
            throw new ObjectDisposedException(nameof(MockRabbitConnectionPool));
        }
    }
}