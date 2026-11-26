using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Contour.Helpers;

internal static class SemaphoreExtensions
{
    public static async ValueTask<T> ExecuteWithCondition<T>(
        this SemaphoreSlim semaphore,
        Func<Task<T>> action,
        Func<bool> condition = null,
        ILogger logger = null,
        CancellationToken cancellationToken = default)
    {
        condition ??= (() => true);

        if (!condition())
        {
            return default;
        }

        cancellationToken.ThrowIfCancellationRequested();

        logger?.LogTrace("Entering the lock");
        await semaphore.WaitAsync(cancellationToken); // In case of exception here no need to release semaphore
        logger?.LogTrace("The lock entered");

        try
        {
            if (condition())
            {
                return await action();
            }

            return default;
        }
        finally
        {
            try
            {
                logger?.LogTrace("Releasing the lock");
                semaphore.Release();
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
        }
    }
}
