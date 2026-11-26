using System;
using System.Collections.Generic;

namespace Contour.Sending;

/// <summary>
/// The publishing options.
/// </summary>
public class PublishingOptions
{
    /// <summary>
    /// Gets or sets the persistently.
    /// </summary>
    public bool? Persistently { get; set; }

    /// <summary>
    /// Gets or sets the ttl.
    /// </summary>
    public TimeSpan? Ttl { get; set; }

    /// <summary>
    /// Gets or sets the ttl.
    /// </summary>
    public TimeSpan? Delay { get; set; }

    /// <summary>
    /// Дополнительные заголовки
    /// </summary>
    public Dictionary<string, object> AdditionalHeaders { get; set; }

    /// <summary>
    /// Префикс заголовка x-breadcrumbs
    /// </summary>
    public string BreadcrumbsPrefix { get; set; }

    /// <summary>
    /// Ключ сообщения. В Kafka сообщения с одним ключом попадают в одну партицию и читаются в порядке отправки.
    /// RabbitMQ ключ не поддерживает.
    /// </summary>
    public string Key { get; set; }

    internal string ForceConnectionKey { get; set; }
}