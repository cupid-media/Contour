using System.Diagnostics;
using Testcontainers.Kafka;

namespace Contour.Kafka.IntegrationTests;

/// <summary>
/// Base class for integration tests that provides shared Kafka TestContainer instance.
/// The container is started once for the entire test run and shared across all test classes.
/// </summary>
[SetUpFixture]
public class KafkaTestFixture
{
    private static KafkaContainer? kafkaContainer;
    private static readonly object LockObject = new();

    /// <summary>
    /// Bootstrap servers in the <c>host:port</c> form expected by <c>librdkafka</c>.
    /// </summary>
    public static string BootstrapServers => StripScheme(
        kafkaContainer?.GetBootstrapAddress() ?? throw new InvalidOperationException("Kafka container is not initialized"));

    public static KafkaContainer Container => kafkaContainer ?? throw new InvalidOperationException("Kafka container is not initialized");

    [OneTimeSetUp]
    public async Task GlobalSetUp()
    {
        Trace.Listeners.Add(new ConsoleTraceListener());
        await StartKafkaContainer();
    }

    [OneTimeTearDown]
    public async Task GlobalTearDown()
    {
        await StopKafkaContainer();
        Trace.Flush();
    }

    private static async Task StartKafkaContainer()
    {
        lock (LockObject)
        {
            if (kafkaContainer != null)
            {
                return;
            }

            TestContext.Progress.WriteLine("Starting Kafka TestContainer...");

            kafkaContainer = new KafkaBuilder()
                .WithImage("confluentinc/cp-kafka:7.6.1")
                .WithStartupCallback((container, ct) =>
                {
                    TestContext.Progress.WriteLine("Kafka container started:");
                    TestContext.Progress.WriteLine($"  - Bootstrap servers: {container.GetBootstrapAddress()}");
                    return Task.CompletedTask;
                })
                .Build();
        }

        await kafkaContainer.StartAsync();

        TestContext.Progress.WriteLine("Kafka TestContainer is ready for tests");
    }

    private static async Task StopKafkaContainer()
    {
        if (kafkaContainer != null)
        {
            TestContext.Progress.WriteLine("Stopping Kafka TestContainer...");
            await kafkaContainer.StopAsync();
            await kafkaContainer.DisposeAsync();
            kafkaContainer = null;
            TestContext.Progress.WriteLine("Kafka TestContainer stopped");
        }
    }

    /// <summary>
    /// <c>GetBootstrapAddress</c> returns the address with the listener scheme (<c>PLAINTEXT://</c>),
    /// which is not what the Confluent client config expects.
    /// </summary>
    private static string StripScheme(string address)
    {
        var separator = address.IndexOf("://", StringComparison.Ordinal);
        return separator < 0 ? address : address[(separator + 3)..];
    }
}
