using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Contour;

internal sealed class MessageHeaderStorage : IIncomingMessageHeaderStorage
{
    /// <summary>
    /// Имя, обозначающее все заголовки сразу: в списке исключаемых запрещает перенос любого
    /// заголовка входящего сообщения в исходящие.
    /// </summary>
    public const string AllHeaders = "*";

    private static readonly AsyncLocal<Dictionary<string, object>> Storage = new();

    private readonly HashSet<string> blacklistedHeaderSet;

    public MessageHeaderStorage(IEnumerable<string> blackHeaders)
    {
        this.blacklistedHeaderSet = new HashSet<string>(blackHeaders);
    }

    /// <summary>
    /// Исключены ли все заголовки без разбора.
    /// </summary>
    private bool AllHeadersExcluded => this.blacklistedHeaderSet.Contains(AllHeaders);

    /// <summary>
    /// Сохраняет заголовки входящего сообщения.
    /// </summary>
    /// <param name="headers">Заголовки входящего сообщения.</param>
    public void Store(IDictionary<string, object> headers)
    {
        var refindedHeaders = this.AllHeadersExcluded
            ? new Dictionary<string, object>()
            : headers
                .Where(p => !this.blacklistedHeaderSet.Contains(p.Key))
                .ToDictionary(p => p.Key, p => p.Value);
        Storage.Value = refindedHeaders;
    }

    /// <summary>
    /// Возвращает сохраненные заголовки входящего сообщения.
    /// </summary>
    /// <returns>Заголовки входящего сообщения.</returns>
    public IDictionary<string, object> Load()
    {
        return Storage.Value;
    }

    /// <summary>
    /// Registers header names to be excluded on storing
    /// </summary>
    /// <param name="headers">Header names</param>
    public void RegisterExcludedHeaders(IEnumerable<string> headers)
    {
        if (headers != null)
        {
            this.blacklistedHeaderSet.UnionWith(headers);
        }
    }

    /// <summary>
    /// Возвращает сохраненные заголовки входящего сообщения.
    /// </summary>
    /// <returns>Заголовки входящего сообщения.</returns>
    public static IDictionary<string, object> LoadStatic()
    {
        return Storage.Value;
    }
}
