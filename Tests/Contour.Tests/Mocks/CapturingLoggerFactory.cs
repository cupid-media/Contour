using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Contour.Tests.Mocks;

/// <summary>
///   Фабрика логгеров, запоминающая предупреждения всех созданных логгеров.
/// </summary>
internal sealed class CapturingLoggerFactory : ILoggerFactory
{
    private readonly ConcurrentQueue<string> warnings = new();

    public IReadOnlyList<string> Warnings => this.warnings.ToList();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(this.warnings);

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Dispose()
    {
    }

    private sealed class CapturingLogger(ConcurrentQueue<string> warnings) : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
            {
                warnings.Enqueue(formatter(state, exception));
            }
        }
    }
}
