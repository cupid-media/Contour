namespace Contour.Configuration;

using System;

/// <summary>
///   Интерфейс для определения зависимостей при конфигурировании клиента шины сообщений.
/// </summary>
public interface IDependencyResolver
{
    /// <summary>
    /// Возвращает компонент по имени и типу.
    /// </summary>
    /// <param name="name">
    /// Имя компонента.
    /// </param>
    /// <param name="specificType">
    /// Тип компонента.
    /// </param>
    /// <returns>
    /// Найденный объект, либо null.
    /// </returns>
    T Resolve<T>(string name, Type specificType = null);
}