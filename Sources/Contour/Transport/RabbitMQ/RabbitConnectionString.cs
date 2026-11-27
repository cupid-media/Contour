using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Contour.Helpers;
using Microsoft.Extensions.Logging;

namespace Contour.Transport.RabbitMQ;

/// <summary>
/// Enables RabbitMQ connection string operations
/// </summary>
public class RabbitConnectionString : IEnumerable<string>
{
    private const string Separator = ",";
    private readonly IList<string> urls;
    private readonly ILogger logger = LogManager.GetLogger<RabbitConnectionString>();

    /// <summary>
    /// Initializes a new instance of the <see cref="RabbitConnectionString"/> class.
    /// </summary>
    /// <param name="connectionString">
    /// The connection string.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// Raises an error if connection string is null or empty
    /// </exception>
    public RabbitConnectionString(string connectionString)
    {
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        this.urls = connectionString.Split([Separator], StringSplitOptions.RemoveEmptyEntries)
            .Distinct()
            .Select(s => s.Trim())
            .Where(s => !string.IsNullOrEmpty(s))
            .ToList();

        if (!this.urls.Any())
        {
            throw new ArgumentException("The provided connection string does not contain any URLs");
        }

        this.logger.LogTrace("Connection string [{ConnectionString}] has been split into: {Urls}", ConnectionStringHelper.Sanitize(connectionString), string.Join("; ", this.urls.Select(ConnectionStringHelper.Sanitize)));
    }

    /// <summary>
    /// Gets the connection strings enumerator
    /// </summary>
    /// <returns>
    /// The <see cref="IEnumerator"/>.
    /// </returns>
    public IEnumerator<string> GetEnumerator()
    {
        return this.urls.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}