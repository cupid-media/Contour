using System.Threading.Tasks;
using Contour.Helpers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Contour;

internal abstract class ConnectionPool<TConnection> : IConnectionPool<TConnection> where TConnection : class, IConnection
{
    private readonly SemaphoreSlim semaphore = new(1, 1);
    private readonly ILogger logger;
    private readonly ConcurrentDictionary<string, IList<(TConnection Connection, bool Reusable)>> groups = new();

    private bool disposed;
    private CancellationTokenSource cancellation;

    protected ConnectionPool()
    {
        this.logger = LogManager.GetLogger(GetType().FullName);
        this.cancellation = new CancellationTokenSource();
    }

    public int Count => this.groups.SelectMany(pair => pair.Value).Count();

    protected IConnectionProvider<TConnection> Provider { get; set; }

    /// <summary>
    /// Gets a new connection from the pool or uses an existing one
    /// </summary>
    /// <param name="connectionString">A connection string to create a new or get an existing connection</param>
    /// <param name="reusable">Specifies if a connection can be reused</param>
    /// <param name="token">Operation cancellation token</param>
    /// <returns>A pooled connection</returns>
    public async Task<TConnection> Get(string connectionString, bool reusable, CancellationToken token)
    {
        return await this.semaphore.ExecuteWithCondition(async () =>
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(typeof(ConnectionPool<TConnection>).Name);
            }

            using var source = CancellationTokenSource.CreateLinkedTokenSource(token, this.cancellation.Token);
            source.Token.ThrowIfCancellationRequested();

            var group = this.groups.GetOrAdd(connectionString, _ => []);

            RemoveNotActiveConnections(group);

            (TConnection Connection, bool Reusable) pair;
            if (reusable && group.Any(t => t.Reusable))
            {
                pair = group.First(t => t.Reusable);
                this.logger.LogTrace("A reusable connection [{Connection}] has been fetched from the pool", pair.Connection);
            }
            else
            {
                var connection = Provider.Create(connectionString);
                this.logger.LogTrace("A new connection [{Connection}] has been created", connection);

                pair = (connection, reusable);
                group.Add(pair);

                await connection.Open(source.Token);
                this.logger.LogTrace("Connection [{Connection}] has been opened", connection);
            }

            return pair.Connection;
        }, logger: this.logger, cancellationToken: token);
    }

    /// <summary>
    /// Cancels any pending connection requests and drops all connections
    /// </summary>
    public async Task Drop()
    {
        this.logger.LogTrace("Dropping connection pool...");

        // Cancel any pending connection requests
        this.cancellation.Cancel();
        this.cancellation.Dispose();
        this.logger.LogTrace("All pending connection requests have been canceled");

        await this.semaphore.ExecuteWithCondition(async () =>
        {
            while (this.groups.Count > 0 && this.groups.TryRemove(this.groups.Keys.First(), out var group))
            {
                this.logger.LogTrace("Cleaning up connection group...");

                while (group.Any())
                {
                    var pair = group.First();
                    try
                    {
                        var connection = pair.Connection;

                        this.logger.LogTrace("Closing connection [{Connection}]...", connection);
                        await connection.Close();
                        this.logger.LogTrace("Connection [{Connection}] closed", connection);

                        this.logger.LogTrace("Disposing connection [{Connection}]...", connection);
                        await connection.DisposeAsync();
                        this.logger.LogTrace("Connection [{Connection}] disposed", connection);
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogWarning(ex, "Failed to dispose a pooled connection due to: {Message}", ex.Message);
                    }
                    finally
                    {
                        group.Remove(pair);
                    }
                }

                this.logger.LogTrace("Connection group cleanup successfully completed");
            }

            // Re-enable the clients to get new connections from the pool
            this.cancellation = new CancellationTokenSource();

            return 0;
        }, logger: this.logger);

        this.logger.LogTrace("Connection pool successfully dropped");
    }

    /// <summary>
    /// Drops all connections and disposes of the pool
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        this.logger.LogTrace("Disposing connection pool...");

        this.disposed = true;
        await Drop();
        this.semaphore.Dispose();

        this.logger.LogTrace("Connection pool successfully disposed");
    }

    private static void RemoveNotActiveConnections(IList<(TConnection Connection, bool Reusable)> group)
    {
        group.Where(c => !c.Connection.IsActive).ToList().ForEach(c => group.Remove(c));
    }
}