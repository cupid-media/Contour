using Contour.Transport.RabbitMQ.Configuration;
using Contour.Transport.RabbitMQ.Internal;
using Contour.Transport.RabbitMQ.Topology;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using Queue = EasyNetQ.Management.Client.Model.Queue;

namespace Contour.RabbitMQ.IntegrationTests;

/// <summary>
/// Base class for integration tests that provides common setup for Contour with RabbitMQ TestContainer.
/// Inherit from this class to get access to a configured bus factory and RabbitMQ connection.
/// </summary>
[TestFixture]
public abstract class RabbitIntegrationTestBase
{
    protected string RabbitMqConnectionString => RabbitMqTestFixture.ConnectionString;

    protected List<IBus> CreatedBuses { get; } = [];

    protected string DefaultEndpointName => GetType().Name;

    protected ManagementClient ManagementClient { get; } = new(RabbitMqTestFixture.ManagementApiUrl, "guest", "guest");

    [OneTimeTearDown]
    public virtual async Task TearDown()
    {
        ManagementClient.Dispose();
        foreach (var bus in CreatedBuses)
        {
            await bus.Stop();
        }
    }

    protected async Task<IBus> CreateBus(string endpointName, Action<RabbitBusConfigurator> config)
    {
        var bus = await BusFactory.CreateRmqBus(cfg =>
        {
            cfg.WithEndpointName(endpointName);
            cfg.WithConnectionString(RabbitMqConnectionString);

            config.Invoke(cfg);
            cfg.AutoStart = false;
        });
        CreatedBuses.Add(bus);
        return bus;
    }

    internal async Task<RabbitBus> CreateAndStartBus(string endpointName, Action<RabbitBusConfigurator> config)
    {
        var bus = await CreateBus(endpointName, config);
        await bus.Start();
        return (RabbitBus)bus;
    }

    protected static string GetRandomQueueName() => "queue" + NameGenerator.GetRandomName(10).ToLower();

    /// <summary>
    /// Helper method to wait for a condition with timeout
    /// </summary>
    protected static async Task<bool> WaitForCondition(Func<Task<bool>> condition, TimeSpan timeout, TimeSpan? checkInterval = null)
    {
        var endTime = DateTime.UtcNow.Add(timeout);

        while (DateTime.UtcNow < endTime)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(checkInterval ?? TimeSpan.FromMilliseconds(50));
        }

        return false;
    }

    protected async Task<bool> HasQueue(string endpointName, string queueName, string vhost = "/")
    {
        try
        {
            await ManagementClient.GetQueueAsync(new QueueName($"{endpointName}.{queueName}", vhost));
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    protected async Task<Queue?> GetQueue(string endpointName, string queueName, string vhost = "/")
    {
        try
        {
            return await ManagementClient.GetQueueAsync(new QueueName($"{endpointName}.{queueName}", vhost));
        }
        catch (Exception)
        {
            return null;
        }
    }
}
