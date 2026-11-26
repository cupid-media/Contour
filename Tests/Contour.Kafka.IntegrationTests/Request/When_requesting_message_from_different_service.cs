using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Request;

/// <summary>
/// Requester and responder as two separate services, each with its own bus.
/// </summary>
public class When_requesting_message_from_different_service : KafkaIntegrationTestBase
{
    private IBus requester = null!;
    private TestKafkaResponder<DummyMessage, DummyMessage> responder = null!;
    private string requestTopic = null!;
    private string derivedReplyTopic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.requestTopic = await CreateTopic();

        this.responder = new TestKafkaResponder<DummyMessage, DummyMessage>(
            m => new DummyMessage { Id = m.Payload.Id + 100 });

        // Отвечающий сервис: только получатель, свой топик ответов ему не нужен.
        await CreateAndStartBus(
            cfg => cfg.On(this.requestTopic).WithReplies().ReactWith(this.responder),
            groupId: $"{DefaultGroupId}-responder");

        // Топик ответов задать на уровне шины нельзя — он производится от имени конечной точки
        // и метки маршрута.
        this.derivedReplyTopic = $"{DefaultEndpointName}.{this.requestTopic}..reply".ToLowerInvariant();

        // Создаётся заранее: консьюмер ответов читает с конца топика, и ответ, пришедший до того,
        // как он увидит автосозданный топик, был бы пропущен.
        await CreateTopic(this.derivedReplyTopic);

        this.requester = await CreateAndStartBus(
            cfg =>
            {
                cfg.Route(this.requestTopic).WithDefaultCallbackEndpoint();
            },
            groupId: $"{DefaultGroupId}-requester");
    }

    [Test]
    public async Task Then_response_comes_from_the_other_service()
    {
        var result = await this.requester.RequestAsync<DummyMessage, DummyMessage>(
            this.requestTopic,
            new DummyMessage { Id = 5 });

        result.Response.Id.Should().Be(105);
    }

    [Test]
    public async Task Then_responder_sees_the_request_headers()
    {
        await this.requester.RequestAsync<DummyMessage, DummyMessage>(
            this.requestTopic,
            new DummyMessage { Id = 1 });

        await WaitForCondition(() => this.responder.Handled.Any());

        var handled = this.responder.Handled.First();

        handled.Headers[Headers.ReplyRoute].Should().Be(this.derivedReplyTopic);
        handled.Headers.Should().ContainKey(Headers.CorrelationId);
    }

    [Test]
    public void Then_requester_cannot_handle_the_request_topic_itself()
    {
        this.requester.CanRoute(this.requestTopic).Should().BeTrue();
        this.requester.CanHandle(this.requestTopic).Should().BeFalse();
    }
}
