using System;

namespace Contour.Configuration;

/// <summary>
/// The dependency resolver func.
/// </summary>
/// <param name="name">
/// The name.
/// </param>
/// <param name="type">
/// The type.
/// </param>
public delegate object DependencyResolverFunc(string name, Type type);

/// <summary>
/// The lambda dependency resolver.
/// </summary>
internal class LambdaDependencyResolver(DependencyResolverFunc resolver) : IDependencyResolver
{
    public T Resolve<T>(string name, Type type = null)
    {
        return (T)resolver(name, type ?? typeof(T));
    }
}