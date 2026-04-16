using System;

namespace Contour.Helpers
{
    internal static class ConnectionStringHelper
    {
        internal static string Sanitize(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                return connectionString;
            }

            try
            {
                var uri = new Uri(connectionString);
                var hostPort = uri.IsDefaultPort ? uri.Host : $"{uri.Host}:{uri.Port}";
                var path = uri.AbsolutePath;
                return $"{uri.Scheme}://{hostPort}{path}";
            }
            catch
            {
                return "***";
            }
        }
    }
}
