using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Reflection;

namespace Contour.Configuration;

/// <summary>
/// Находит тип сообщения по имени, указанному в конфигурационной секции.
/// </summary>
internal sealed class MessageTypeResolver
{
    private readonly Lazy<List<Type>> loadedTypes = new(LoadTypes);

    public Type Resolve(string messageType)
    {
        var type = Type.GetType(messageType);

        if (type != null)
        {
            return type;
        }

        type = this.loadedTypes.Value.FirstOrDefault(t => t.Name == messageType);

        if (type != null)
        {
            return type;
        }

        throw new ConfigurationErrorsException($"Unknown type [{messageType}]");
    }

    private static List<Type> LoadTypes()
    {
        return AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic)
            .SelectMany(a =>
            {
                try
                {
                    return a.GetExportedTypes();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    // Сборку могло не получиться загрузить целиком — берём то, что доступно.
                    return ex.Types.Where(t => t != null)!;
                }
            })
            .ToList();
    }
}
