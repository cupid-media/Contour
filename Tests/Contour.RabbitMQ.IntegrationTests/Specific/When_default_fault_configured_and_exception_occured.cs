using Contour.RabbitMQ.IntegrationTests.Helpers;
using EasyNetQ.Management.Client;
using EasyNetQ.Management.Client.Model;
using FluentAssertions;
using Newtonsoft.Json;

namespace Contour.RabbitMQ.IntegrationTests.Specific;

public class When_default_fault_configured_and_exception_occured : RabbitIntegrationTestBase
{
    private string testIncomingQueueName;

    private TaskCompletionSource tcs;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.tcs = new TaskCompletionSource();
        this.testIncomingQueueName = GetRandomQueueName();

        var bus1 = await CreateAndStartBus(DefaultEndpointName, cfg =>
        {
            cfg.UseDefaultFaultStrategy();
            cfg.On(this.testIncomingQueueName)
                .ReactWith(new TestConsumer<DummyMessage>(context =>
                {
                    this.tcs.SetResult();
                    throw new CustomException();
                }));
            cfg.Route(this.testIncomingQueueName);
        });

        await bus1.EmitAsync(this.testIncomingQueueName, new DummyMessage());

        await this.tcs.Task;
        await Task.Delay(3000);
    }

    [Test]
    public async Task Then_message_is_forwarded_to_fault_queue()
    {
        await WaitForCondition(() => HasQueue(DefaultEndpointName, "Fault"), TimeSpan.FromSeconds(10));
        var messages = await ManagementClient.GetMessagesFromQueueAsync("/", $"{DefaultEndpointName}.Fault", new GetMessagesFromQueueInfo(10, AckMode.AckRequeueFalse));
        messages.Should().HaveCount(1);
        var message = messages[0];
        var headers = message.Properties["headers"] as IReadOnlyDictionary<string, object>;
        HeadersHelper.ReadHeaderValue(headers["x-message-type"]).Should().Be("document.contour.failed");
        HeadersHelper.ReadHeaderValue(headers["x-breadcrumbs"]).Should().Be($"{DefaultEndpointName};{DefaultEndpointName}");
        var faultMessage = JsonConvert.DeserializeObject<FaultMessage>(message.Payload);
        faultMessage.ContentType.Should().Be("application/json");
        faultMessage.Exception.Should().BeOfType<FaultException>();
        faultMessage.Payload.Should().Be("{\"Id\":0}");
        faultMessage.Label.Should().Be(this.testIncomingQueueName);
    }

    public class CustomException : Exception{}
}
