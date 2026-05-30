using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;
using NotificationDispatch.Infrastructure;
using NotificationDispatch.Integration.Fixtures;
using NotificationDispatch.Worker.Senders;
using NotificationDispatch.Worker.Services;
using StackExchange.Redis;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;

// DeliveryState serializes as integer by default (no JsonStringEnumConverter on the API).
// Helper constants to keep assertions readable.
// Queued=0, Processing=1, Delivered=2, DeadLettered=3

namespace NotificationDispatch.Integration;

public class EndToEndTests : IClassFixture<AppFixture>
{
    private readonly AppFixture _app;

    public EndToEndTests(AppFixture app)
    {
        _app = app;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private async Task<HttpResponseMessage> PostNotificationAsync(object body, string? idempotencyKey = null)
    {
        idempotencyKey ??= Guid.NewGuid().ToString();
        var request = new HttpRequestMessage(HttpMethod.Post, "/notifications")
        {
            Content = JsonContent.Create(body)
        };
        request.Headers.Add("Idempotency-Key", idempotencyKey);
        return await _app.ApiClient.SendAsync(request);
    }

    private async Task<string> GetJobIdAsync(HttpResponseMessage response)
    {
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        return body.GetProperty("jobId").GetString()!;
    }

    // State is serialized as integer: Queued=0, Processing=1, Delivered=2, DeadLettered=3
    private async Task<JsonElement> WaitForStateAsync(string jobId, DeliveryState expectedState,
        TimeSpan? timeout = null)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTimeOffset.UtcNow < deadline)
        {
            var response = await _app.ApiClient.GetAsync($"/notifications/{jobId}");
            if (response.IsSuccessStatusCode)
            {
                var status = await response.Content.ReadFromJsonAsync<JsonElement>();
                if (status.GetProperty("state").GetInt32() == (int)expectedState)
                    return status;
            }
            await Task.Delay(200);
        }
        throw new TimeoutException($"Job {jobId} did not reach state '{expectedState}' within {timeout ?? TimeSpan.FromSeconds(10)}");
    }

    private (NotificationWorkerService worker, CancellationTokenSource cts) StartWorker(
        params INotificationSender[] senders)
    {
        var connection = ConnectionMultiplexer.Connect(_app.RedisConnectionString);
        var worker = new NotificationWorkerService(
            connection,
            new SenderRouter(senders),
            new RedisStatusStore(connection),
            new DeadLetterStore(connection),
            new RetryPolicy(),
            NullLogger<NotificationWorkerService>.Instance);
        var cts = new CancellationTokenSource();
        _ = worker.StartAsync(cts.Token);
        return (worker, cts);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PostNotification_Returns202_AndJobIsQueued()
    {
        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "e2e@example.com",
            body = "scenario 1"
        });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var jobId = await GetJobIdAsync(response);
        Assert.False(string.IsNullOrEmpty(jobId));

        var statusResponse = await _app.ApiClient.GetAsync($"/notifications/{jobId}");
        Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
        var status = await statusResponse.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal((int)DeliveryState.Queued, status.GetProperty("state").GetInt32());
    }

    [Fact]
    public async Task EmailJob_ProcessedByWorker_ReachesDelivered()
    {
        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "delivered@example.com",
            body = "scenario 2"
        });
        var jobId = await GetJobIdAsync(response);

        var (worker, cts) = StartWorker(new EmailSender(NullLogger<EmailSender>.Instance));
        try
        {
            var status = await WaitForStateAsync(jobId, DeliveryState.Delivered);
            Assert.Equal(1, status.GetProperty("attempts").GetInt32());
            Assert.True(status.TryGetProperty("completedAt", out var completedAt)
                && completedAt.ValueKind != JsonValueKind.Null);
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task FailingWebhook_DeadLettersAfterRetries_AndWritesToDlq()
    {
        using var mockServer = WireMockServer.Start();
        mockServer.Given(Request.Create().WithPath("/fail").UsingPost())
                  .RespondWith(Response.Create().WithStatusCode(500).WithBody("internal error"));

        var response = await PostNotificationAsync(new
        {
            channel = "webhook",
            recipient = $"{mockServer.Url}/fail",
            body = "scenario 3"
        });
        var jobId = await GetJobIdAsync(response);

        var webhookSender = new WebhookSender(new HttpClient(), NullLogger<WebhookSender>.Instance);
        var (worker, cts) = StartWorker(webhookSender);
        try
        {
            // 3 retries × up to 4s each = ~12s worst case; give 25s
            var status = await WaitForStateAsync(jobId, DeliveryState.DeadLettered, TimeSpan.FromSeconds(25));
            Assert.Equal((int)DeliveryState.DeadLettered, status.GetProperty("state").GetInt32());
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // DLQ entry should exist via the API
        var dlqResponse = await _app.ApiClient.GetAsync("/notifications/dlq?count=50");
        Assert.Equal(HttpStatusCode.OK, dlqResponse.StatusCode);
        var entries = await dlqResponse.Content.ReadFromJsonAsync<JsonElement[]>();
        Assert.NotNull(entries);
        Assert.Contains(entries, e =>
            e.TryGetProperty("jobId", out var id) && id.GetString() == jobId);
    }

    [Fact]
    public async Task DlqReplay_ViaApi_CreatesNewQueuedJob()
    {
        // Arrange: dead-letter a webhook job
        using var mockServer = WireMockServer.Start();
        mockServer.Given(Request.Create().WithPath("/fail").UsingPost())
                  .RespondWith(Response.Create().WithStatusCode(500));

        var response = await PostNotificationAsync(new
        {
            channel = "webhook",
            recipient = $"{mockServer.Url}/fail",
            body = "scenario 4"
        });
        var originalJobId = await GetJobIdAsync(response);

        var webhookSender = new WebhookSender(new HttpClient(), NullLogger<WebhookSender>.Instance);
        var (worker, cts) = StartWorker(webhookSender);
        try
        {
            await WaitForStateAsync(originalJobId, DeliveryState.DeadLettered, TimeSpan.FromSeconds(25));
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // Act: call replay endpoint
        var replayResponse = await _app.ApiClient.PostAsync(
            $"/notifications/dlq/{originalJobId}/replay", null);

        Assert.Equal(HttpStatusCode.Accepted, replayResponse.StatusCode);
        var replayBody = await replayResponse.Content.ReadFromJsonAsync<JsonElement>();
        var newJobId = replayBody.GetProperty("newJobId").GetString()!;
        Assert.NotEqual(originalJobId, newJobId);

        // Assert: new job is Queued (not yet processed)
        var statusResponse = await _app.ApiClient.GetAsync($"/notifications/{newJobId}");
        Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
        var status = await statusResponse.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal((int)DeliveryState.Queued, status.GetProperty("state").GetInt32());
    }

    [Fact]
    public async Task DuplicateIdempotencyKey_ReturnsSameJobId_With200()
    {
        var key = Guid.NewGuid().ToString();
        var body = new { channel = "email", recipient = "idem@example.com", body = "scenario 5" };

        var first = await PostNotificationAsync(body, key);
        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        var firstJobId = await GetJobIdAsync(first);

        var second = await PostNotificationAsync(body, key);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        var secondBody = await second.Content.ReadFromJsonAsync<JsonElement>();
        var secondJobId = secondBody.GetProperty("jobId").GetString()!;

        Assert.Equal(firstJobId, secondJobId);
    }
}
