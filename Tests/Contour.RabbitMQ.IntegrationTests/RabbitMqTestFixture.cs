using System.Diagnostics;
using DotNet.Testcontainers.Builders;
using Testcontainers.RabbitMq;

namespace Contour.RabbitMQ.IntegrationTests;

/// <summary>
/// Base class for integration tests that provides shared RabbitMQ TestContainer instance.
/// The container is started once for the entire test run and shared across all test classes.
/// </summary>
[SetUpFixture]
public class RabbitMqTestFixture
{
    private static RabbitMqContainer? rabbitMqContainer;
    private static readonly object LockObject = new();

    public static string ConnectionString => rabbitMqContainer?.GetConnectionString() ?? throw new InvalidOperationException("RabbitMQ container is not initialized");

    public static Uri ManagementApiUrl => rabbitMqContainer?.Hostname != null
        ? new Uri($"http://localhost:{rabbitMqContainer.GetMappedPublicPort(15672)}")
        : throw new InvalidOperationException("RabbitMQ container is not initialized");

    public static RabbitMqContainer Container => rabbitMqContainer ?? throw new InvalidOperationException("RabbitMQ container is not initialized");

    [OneTimeSetUp]
    public async Task GlobalSetUp()
    {
        Trace.Listeners.Add(new ConsoleTraceListener());
        await StartRabbitMqContainer();
    }

    [OneTimeTearDown]
    public async Task GlobalTearDown()
    {
        await StopRabbitMqContainer();
        Trace.Flush();
    }

    private static async Task StartRabbitMqContainer()
    {
        lock (LockObject)
        {
            if (rabbitMqContainer != null)
            {
                return;
            }

            TestContext.Progress.WriteLine("Starting RabbitMQ TestContainer...");

            rabbitMqContainer = new RabbitMqBuilder()
                .WithImage("rabbitmq:3.13-management")
                .WithUsername("guest")
                .WithPassword("guest")
                .WithPortBinding(5672, true) // Random host port
                .WithPortBinding(15672, true) // Management UI
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilMessageIsLogged("Server startup complete"))
                .WithStartupCallback((container, ct) =>
                {
                    TestContext.Progress.WriteLine($"RabbitMQ container started:");
                    TestContext.Progress.WriteLine($"  - AMQP: {container.GetConnectionString()}");
                    TestContext.Progress.WriteLine($"  - Management UI: http://localhost:{container.GetMappedPublicPort(15672)}");
                    TestContext.Progress.WriteLine($"  - Username: guest, Password: guest");
                    return Task.CompletedTask;
                })
                .Build();
        }

        await rabbitMqContainer.StartAsync();

        // Wait a bit for RabbitMQ to be fully ready
        await Task.Delay(TimeSpan.FromSeconds(3));

        TestContext.Progress.WriteLine("RabbitMQ TestContainer is ready for tests");
    }

    private static async Task StopRabbitMqContainer()
    {
        if (rabbitMqContainer != null)
        {
            TestContext.Progress.WriteLine("Stopping RabbitMQ TestContainer...");
            await rabbitMqContainer.StopAsync();
            await rabbitMqContainer.DisposeAsync();
            rabbitMqContainer = null;
            TestContext.Progress.WriteLine("RabbitMQ TestContainer stopped");
        }
    }
}
