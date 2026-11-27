using System;
using System.Linq;

namespace Contour.Helpers;

/// <summary>
/// Готовит строку подключения к брокеру для записи в лог.
/// </summary>
internal static class ConnectionStringHelper
{
    private const string Unparsed = "***";

    /// <summary>
    /// Убирает из строки подключения логин, пароль и параметры запроса: остаются схема, хост, порт и
    /// путь. Строка из нескольких адресов через запятую обрабатывается по адресам.
    /// </summary>
    /// <param name="connectionString">Строка подключения: один адрес или несколько через запятую.</param>
    /// <returns>Строка, которую можно писать в лог.</returns>
    internal static string Sanitize(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return connectionString;
        }

        var urls = connectionString.Split([','], StringSplitOptions.RemoveEmptyEntries);

        return string.Join(",", urls.Select(url => SanitizeUrl(url.Trim())));
    }

    private static string SanitizeUrl(string url)
    {
        // Без схемы Uri принимает "логин:пароль@хост" за что угодно, кроме адреса: такой адрес не разбираем.
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || string.IsNullOrEmpty(uri.Host))
        {
            return Unparsed;
        }

        var hostPort = uri.IsDefaultPort ? uri.Host : $"{uri.Host}:{uri.Port}";

        return $"{uri.Scheme}://{hostPort}{uri.AbsolutePath}";
    }
}
