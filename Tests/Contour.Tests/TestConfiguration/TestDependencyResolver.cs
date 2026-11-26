using System;
using System.Collections.Generic;
using System.Linq;

namespace Contour.Tests.TestConfiguration;

public class TestDependencyResolver
{
    private readonly Dictionary<(string name, Type type), object> dependencies = new();

    public void Register<T>(string name, T instance)
    {
        this.dependencies[(name, typeof(T))] = instance;
    }

    public object Resolve(string name, Type type)
    {
        if (this.dependencies.TryGetValue((name, type), out var instance))
        {
            return instance;
        }

        // Try to find compatible type
        var compatibleKey = this.dependencies.Keys.FirstOrDefault(k => k.name == name && type.IsAssignableFrom(k.type));
        if (compatibleKey != default)
        {
            return this.dependencies[compatibleKey];
        }

        throw new InvalidOperationException($"Unable to resolve dependency: {name} of type {type}");
    }
}
