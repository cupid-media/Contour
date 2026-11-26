using System;
using System.Threading;
using System.Threading.Tasks;

namespace Contour;

internal interface IConnectionPool<TConnection> : IAsyncDisposable where TConnection : class, IConnection
{
    int Count { get; }

    Task<TConnection> Get(string connectionString, bool reusable, CancellationToken token);

    Task Drop();
}