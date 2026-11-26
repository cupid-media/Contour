using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using KafkaHeaders = Confluent.Kafka.Headers;

namespace Contour.Transport.Kafka.Internal;

/// <summary>
///   Преобразование заголовков Contour (<c>object</c>) в заголовки Kafka (<c>byte[]</c>) и обратно.
///   Kafka не типизирует заголовки, поэтому значения кодируются инвариантной строкой в UTF-8,
///   а на приёме возвращаются строками.
/// </summary>
internal static class KafkaHeaderConverter
{
    public static KafkaHeaders ToKafka(IDictionary<string, object> headers)
    {
        var kafkaHeaders = new KafkaHeaders();

        if (headers == null)
        {
            return kafkaHeaders;
        }

        foreach (var header in headers)
        {
            var value = Encode(header.Value);

            if (value != null)
            {
                kafkaHeaders.Add(header.Key, value);
            }
        }

        return kafkaHeaders;
    }

    public static IDictionary<string, object> FromKafka(KafkaHeaders headers)
    {
        var result = new Dictionary<string, object>();

        if (headers == null)
        {
            return result;
        }

        foreach (var header in headers)
        {
            var value = header.GetValueBytes();

            // Последний заголовок с тем же именем выигрывает: Kafka допускает дубликаты, Contour — нет.
            result[header.Key] = value == null ? null : Encoding.UTF8.GetString(value);
        }

        return result;
    }

    private static byte[] Encode(object value)
    {
        switch (value)
        {
            case null:
                return null;
            case byte[] bytes:
                return bytes;
            case string text:
                return Encoding.UTF8.GetBytes(text);
            case bool flag:
                // Инвариантный нижний регистр вместо "True"/"False" от bool.ToString().
                return Encoding.UTF8.GetBytes(flag ? "true" : "false");
            case IFormattable formattable:
                return Encoding.UTF8.GetBytes(formattable.ToString(null, CultureInfo.InvariantCulture));
            default:
                return Encoding.UTF8.GetBytes(value.ToString() ?? string.Empty);
        }
    }
}
