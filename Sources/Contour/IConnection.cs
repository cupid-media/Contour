using System;
using System.Threading;
using System.Threading.Tasks;

namespace Contour;

/// <summary>
/// Describes a bus connection
/// </summary>
public interface IConnection : IAsyncDisposable
{
    /// <summary>
    /// Connection identifier
    /// </summary>
    Guid Id { get; }

    bool IsActive { get; }

    /// <summary>
    /// Connection string
    /// </summary>
    string ConnectionString { get; }

    /// <summary>
    /// Ключ строки подключения, который может идентифицировать подключение по параметрам из connString, для RMQ IP + Port + vhost
    /// </summary>
    string ConnectionKey { get; }

    /// <summary>
    /// Opens the connection
    /// </summary>
    /// <param name="token">A token to cancel the operation</param>
    Task Open(CancellationToken token);

    /// <summary>
    /// Closes the connection
    /// </summary>
    Task Close();

    /// <summary>
    /// Aborts the connection
    /// </summary>
    Task Abort(CancellationToken token);
}