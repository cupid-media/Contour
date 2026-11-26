using System.Text.RegularExpressions;

namespace Contour;

/// <summary>
///   Метка сообщения.
/// </summary>
public sealed record MessageLabel
{
    /// <summary>
    /// The alias prefix.
    /// </summary>
    internal const string AliasPrefix = ":";

    /// <summary>
    /// The any name.
    /// </summary>
    private const string AnyName = "*";

    /// <summary>
    /// The empty name.
    /// </summary>
    private const string EmptyName = "";

    /// <summary>
    /// The alias validator.
    /// </summary>
    private static readonly Regex AliasValidator = new(@"\A^:[a-zA-Z_][a-zA-Z\d-_.:]*\z", RegexOptions.Singleline | RegexOptions.Compiled);

    /// <summary>
    /// The label validator.
    /// </summary>
    private static readonly Regex LabelValidator = new(@"\A^[a-zA-Z_*][a-zA-Z\d-_.:]*\z", RegexOptions.Singleline | RegexOptions.Compiled);

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="MessageLabel"/>.
    /// </summary>
    /// <param name="name">
    /// The name.
    /// </param>
    private MessageLabel(string name)
    {
        Name = (name ?? EmptyName).ToLower();
    }

    /// <summary>
    ///   Метка без имени. Используется там, где метка ожидается, но имя не важно.
    /// </summary>
    public static MessageLabel Any => new(AnyName);

    /// <summary>
    ///   Пустая метка.
    /// </summary>
    public static MessageLabel Empty => new(EmptyName);

    /// <summary>
    ///   Показывает, является ли метка псевдонимом.
    /// </summary>
    public bool IsAlias => Name.StartsWith(AliasPrefix);

    /// <summary>
    ///   Показывает, привязана ли метка к конкретному имени.
    /// </summary>
    public bool IsAny => Equals(Any);

    /// <summary>
    ///   Показывает, является ли метка пустой (отсутствующей).
    /// </summary>
    public bool IsEmpty => Equals(Empty);

    /// <summary>
    ///   Название метки.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Создает экземпляр метки сообщения.
    /// </summary>
    /// <param name="name">
    /// Название метки.
    /// </param>
    /// <returns>
    /// The <see cref="MessageLabel"/>.
    /// </returns>
    public static MessageLabel From(string name)
    {
        return new MessageLabel(name);
    }

    /// <summary>
    /// The is valid alias.
    /// </summary>
    /// <param name="alias">
    /// The alias.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public static bool IsValidAlias(string alias)
    {
        return AliasValidator.IsMatch(alias);
    }

    /// <summary>
    /// The is valid label.
    /// </summary>
    /// <param name="label">
    /// The label.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public static bool IsValidLabel(string label)
    {
        if (label.Equals(EmptyName))
        {
            return true;
        }

        return LabelValidator.IsMatch(label);
    }

    /// <summary>
    /// The get hash code.
    /// </summary>
    /// <returns>
    /// The <see cref="int"/>.
    /// </returns>
    public override int GetHashCode()
    {
        return Name != null ? Name.GetHashCode() : 0;
    }

    /// <summary>
    /// The to string.
    /// </summary>
    /// <returns>
    /// The <see cref="string"/>.
    /// </returns>
    public override string ToString()
    {
        return Name;
    }

    /// <summary>
    /// The equals.
    /// </summary>
    /// <param name="other">
    /// The other.
    /// </param>
    /// <returns>
    /// The <see cref="bool"/>.
    /// </returns>
    public bool Equals(MessageLabel other)
    {
        return string.Equals(Name, other?.Name);
    }

    public static implicit operator string(MessageLabel l) => l.Name;

    public static explicit operator MessageLabel(string l) => MessageLabel.From(l);
}