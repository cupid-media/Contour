namespace Contour.Helpers;

using System;
using System.Collections.Generic;

/// <summary>
/// The enumerable ex.
/// </summary>
public static class EnumerableEx
{
    /// <summary>
    /// The for each.
    /// </summary>
    /// <param name="source">
    /// The source.
    /// </param>
    /// <param name="action">
    /// The action.
    /// </param>
    /// <typeparam name="T">
    /// </typeparam>
    public static void ForEach<T>(this IEnumerable<T> source, Action<T> action)
    {
        foreach (T item in source)
        {
            action(item);
        }
    }
}
