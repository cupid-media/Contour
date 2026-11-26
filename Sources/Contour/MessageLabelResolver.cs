using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Contour.Configuration;
using Contour.Helpers;

namespace Contour;

/// <summary>
/// The message label resolver.
/// </summary>
internal class MessageLabelResolver
{
    /// <summary>
    /// The _type labels.
    /// </summary>
    private readonly IDictionary<Type, MessageLabel> typeLabels = new ConcurrentDictionary<Type, MessageLabel>();

    /// <summary>
    /// The resolve from.
    /// </summary>
    /// <param name="payloadType">
    /// The payload type.
    /// </param>
    /// <returns>
    /// The <see cref="MessageLabel"/>.
    /// </returns>
    public MessageLabel ResolveFrom(Type payloadType)
    {
        if (!this.typeLabels.TryGetValue(payloadType, out var label))
        {
            label = GetLabelFrom(payloadType);
            this.typeLabels[payloadType] = label;
        }

        return label;
    }

    /// <summary>
    /// The resolve from.
    /// </summary>
    /// <typeparam name="T">
    /// </typeparam>
    /// <returns>
    /// The <see cref="MessageLabel"/>.
    /// </returns>
    public MessageLabel ResolveFrom<T>() where T : class
    {
        return ResolveFrom(typeof(T));
    }

    /// <summary>
    /// The get label from.
    /// </summary>
    /// <param name="payloadType">
    /// The payload type.
    /// </param>
    /// <returns>
    /// The <see cref="MessageLabel"/>.
    /// </returns>
    /// <exception cref="BusConfigurationException">
    /// </exception>
    private static MessageLabel GetLabelFrom(Type payloadType)
    {
        var attribute = payloadType.GetCustomAttributes(typeof(MessageAttribute), false).SingleOrDefault();
        if (attribute == null)
        {
            throw new BusConfigurationException("No MessageAttribute is defined for class [{0}].".FormatEx(payloadType.Name));
        }

        return ((MessageAttribute)attribute).Label.ToMessageLabel();
    }
}