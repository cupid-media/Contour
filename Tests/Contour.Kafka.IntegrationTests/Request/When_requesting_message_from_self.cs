using Contour.Kafka.IntegrationTests.Helpers;
using Contour.Sending;
using Contour.Transport.Kafka;
using FluentAssertions;

namespace Contour.Kafka.IntegrationTests.Request;

/// <summary>
/// One bus both answers requests and makes them, against a real broker.
/// </summary>
public class When_requesting_message_from_self : KafkaIntegrationTestBase
{
    private IBus bus = null!;
    private string requestTopic = null!;
    private string replyTopic = null!;

    [OneTimeSetUp]
    public async Task Setup()
    {
        this.requestTopic = await CreateTopic();
        this.replyTopic = await CreateTopic();

        this.bus = await CreateAndStartBus(cfg =>
        {
            // Топик ответов задаётся маршруту явно: создан заранее, так что первый poll
            // не зависит от автосоздания.
            cfg.Route(this.requestTopic)
                .WithCallbackTopic(this.replyTopic)
                .WithRequestTimeout(TimeSpan.FromSeconds(30));

            cfg.On(this.requestTopic).WithReplies().ReactWith(
                new TestKafkaResponder<DummyMessage, DummyMessage>(m => new DummyMessage { Id = m.Payload.Id * 2 }));
        });
    }

    [Test]
    public async Task Then_response_is_received()
    {
        var result = await this.bus.RequestAsync<DummyMessage, DummyMessage>(
            this.requestTopic,
            new DummyMessage { Id = 21 });

        result.Response.Should().NotBeNull();
        result.Response.Id.Should().Be(42);
    }

    [Test]
    public async Task Then_multiple_requests_get_their_own_responses()
    {
        var results = await Task.WhenAll(
            Enumerable.Range(1, 5).Select(id =>
                this.bus.RequestAsync<DummyMessage, DummyMessage>(this.requestTopic, new DummyMessage { Id = id })));

        results.Select(r => r.Response.Id).Should().BeEquivalentTo([2, 4, 6, 8, 10]);
    }

    [Test]
    public async Task Then_request_times_out_when_the_topic_has_no_responder()
    {
        var silentTopic = await CreateTopic();

        var silentBus = await CreateAndStartBus(
            cfg =>
            {
                cfg.Route(silentTopic).WithCallbackTopic(this.replyTopic);
            },
            groupId: $"{DefaultGroupId}-silent");

        var request = () => silentBus.RequestAsync<DummyMessage, DummyMessage>(
            silentTopic,
            new DummyMessage { Id = 1 },
            new RequestOptions { Timeout = TimeSpan.FromSeconds(3) });

        await request.Should().ThrowAsync<TimeoutException>();
    }
}
