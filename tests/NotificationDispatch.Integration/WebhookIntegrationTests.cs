using NotificationDispatch.Core.Models;
using NotificationDispatch.Worker.Senders;
using Microsoft.Extensions.Logging.Abstractions;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;

namespace NotificationDispatch.Integration;

public class WebhookIntegrationTests : IAsyncLifetime
{
    private WireMockServer _mockServer = null!;
    private HttpClient _httpClient = null!;

    public ValueTask InitializeAsync()
    {
        _mockServer = WireMockServer.Start();
        _httpClient = new HttpClient();
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _httpClient.Dispose();
        _mockServer.Dispose();
        return ValueTask.CompletedTask;
    }

    [Fact]
    public async Task WebhookSender_SuccessfulDelivery_Returns200()
    {
        _mockServer.Given(
            Request.Create().WithPath("/webhook").UsingPost()
        ).RespondWith(
            Response.Create().WithStatusCode(200)
        );

        var sender = new WebhookSender(
            _httpClient,
            NullLogger<WebhookSender>.Instance);

        var job = new NotificationJob
        {
            JobId = Guid.NewGuid().ToString(),
            Request = new NotificationRequest
            {
                Channel = "webhook",
                Recipient = $"{_mockServer.Url}/webhook",
                Body = "test payload"
            },
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        await sender.SendAsync(job, TestContext.Current.CancellationToken);

        var entry = Assert.Single(_mockServer.LogEntries);
        Assert.NotNull(entry.RequestMessage);
        Assert.Equal("POST", entry.RequestMessage.Method);
        Assert.Equal("/webhook", entry.RequestMessage.Path);
    }

    [Fact]
    public async Task WebhookSender_ServerError_ThrowsHttpRequestException()
    {
        _mockServer.Given(
            Request.Create().WithPath("/webhook-fail").UsingPost()
        ).RespondWith(
            Response.Create().WithStatusCode(500)
        );

        var sender = new WebhookSender(
            _httpClient,
            NullLogger<WebhookSender>.Instance);

        var job = new NotificationJob
        {
            JobId = Guid.NewGuid().ToString(),
            Request = new NotificationRequest
            {
                Channel = "webhook",
                Recipient = $"{_mockServer.Url}/webhook-fail",
                Body = "will fail"
            },
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        await Assert.ThrowsAsync<HttpRequestException>(() =>
            sender.SendAsync(job, TestContext.Current.CancellationToken));
    }
}
