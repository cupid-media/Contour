using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Contour;

internal static class LogManager
{
    private static ILoggerFactory loggerFactory = NullLoggerFactory.Instance;

    public static void Initialize(ILoggerFactory loggerFactory)
    {
        LogManager.loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
    }

    public static ILogger<T> GetLogger<T>()
    {
        return loggerFactory.CreateLogger<T>();
    }

    public static ILogger GetLogger(string categoryName)
    {
        return loggerFactory.CreateLogger(categoryName);
    }
}
