using System.Security.Cryptography;
using System.Text;

namespace Contour.Transport.RabbitMQ.Topology;

/// <summary>
/// Генератор имен элементов топологии.
/// </summary>
internal static class NameGenerator
{
    /// <summary>
    /// Возвращает имя состоящее из случайного набора цифр и букв.
    /// </summary>
    /// <param name="size">Необходимое количество символов в имени.</param>
    /// <returns>Имя из случайного набора цифр и букв.</returns>
    internal static string GetRandomName(int size)
    {
        using var cryptoProvider = RandomNumberGenerator.Create();
        var randomBytes = new byte[(size + 1) / 2];
        cryptoProvider.GetBytes(randomBytes);
        var builder = new StringBuilder();

        foreach (var randomByte in randomBytes)
        {
            builder.Append(randomByte.ToString("X2"));
        }

        return builder.ToString()[..size];
    }
}