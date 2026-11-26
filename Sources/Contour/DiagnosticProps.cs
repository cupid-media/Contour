using System.Collections.Concurrent;
using System.Threading;

namespace Contour;

public static class DiagnosticProps
{
    private static readonly AsyncLocal<ConcurrentDictionary<string, string>> Storage = new();

    public static void EnsureCreated()
    {
        Storage.Value ??= new ConcurrentDictionary<string, string>();
    }

    public static void Store(string name, string value)
    {
        EnsureCreated();
        Storage.Value[name] = value;
    }

    public static string Get(string name)
    {
        if (Storage.Value == null)
        {
            return null;
        }

        if (Storage.Value.ContainsKey(name))
        {
            return Storage.Value[name];
        }

        return null;
    }

    public static class Names
    {
        public static readonly string LastPublishAttemptConnectionString = "LastPublishAttemptConnectionString";

        public static readonly string ConsumerConnectionString = "ConsumerConnectionString";

        public static readonly string Breadcrumbs = "Breadcrumbs";

        public static readonly string OriginalMessageId = "OriginalMessageId";
    }
}