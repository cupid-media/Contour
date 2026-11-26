using System.Text;

namespace Contour.RabbitMQ.IntegrationTests.Helpers;

public static class HeadersHelper
{
    public static string ReadHeaderValue(object value)
    {
        if (value is byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        if (value is long vlong)
        {
            return vlong.ToString();
        }

        return value as string;
    }
}
