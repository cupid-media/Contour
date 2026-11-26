using RabbitMQ.Client;

namespace Contour.Transport.RabbitMQ.Topology;

/// <summary>
/// The exchange builder.
/// </summary>
public class ExchangeBuilder
{
    public const string DelayedExchangeType = "x-delayed-message";
    public const string DelayedExchangeSubtypeArgumentName = "x-delayed-type";

    /// <summary>
    /// Инициализирует новый экземпляр класса <see cref="ExchangeBuilder"/>.
    /// </summary>
    /// <param name="name">
    /// The name.
    /// </param>
    internal ExchangeBuilder(string name)
    {
        Instance = new Exchange(name);
    }

    /// <summary>
    /// Gets the auto delete.
    /// </summary>
    public ExchangeBuilder AutoDelete
    {
        get
        {
            Instance.AutoDelete = true;
            return this;
        }
    }

    /// <summary>
    /// Gets the direct.
    /// </summary>
    public ExchangeBuilder Direct
    {
        get
        {
            Instance.Type = ExchangeType.Direct;
            return this;
        }
    }

    /// <summary>
    /// Gets the durable.
    /// </summary>
    public ExchangeBuilder Durable
    {
        get
        {
            Instance.Durable = true;
            return this;
        }
    }

    /// <summary>
    /// Gets the fanout.
    /// </summary>
    public ExchangeBuilder Fanout
    {
        get
        {
            Instance.Type = ExchangeType.Fanout;
            return this;
        }
    }

    /// <summary>
    /// Gets the topic.
    /// </summary>
    public ExchangeBuilder Topic
    {
        get
        {
            Instance.Type = ExchangeType.Topic;
            return this;
        }
    }

    public ExchangeBuilder DelayedDirect
    {
        get
        {
            Instance.Type = DelayedExchangeType;
            Instance.Arguments[DelayedExchangeSubtypeArgumentName] = ExchangeType.Direct;
            return this;
        }
    }

    public ExchangeBuilder DelayedFanout
    {
        get
        {
            Instance.Type = DelayedExchangeType;
            Instance.Arguments[DelayedExchangeSubtypeArgumentName] = ExchangeType.Fanout;
            return this;
        }
    }

    public ExchangeBuilder DelayedTopic
    {
        get
        {
            Instance.Type = DelayedExchangeType;
            Instance.Arguments[DelayedExchangeSubtypeArgumentName] = ExchangeType.Topic;
            return this;
        }
    }

    /// <summary>
    /// Gets the instance.
    /// </summary>
    internal Exchange Instance { get; private set; }

    /// <summary>
    /// The of type.
    /// </summary>
    /// <param name="type">
    /// The type.
    /// </param>
    /// <returns>
    /// The <see cref="ExchangeBuilder"/>.
    /// </returns>
    public ExchangeBuilder OfType(string type)
    {
        Instance.Type = type;
        return this;
    }
}