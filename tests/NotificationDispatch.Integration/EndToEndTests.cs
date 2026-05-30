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
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;

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

    private async Task<JsonElement> WaitForStateAsync(string jobId, string expectedState,
        TimeSpan? timeout = null)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTimeOffset.UtcNow < deadline)
        {
            var response = await _app.ApiClient.GetAsync($"/notifications/{jobId}");
            if (response.IsSuccessStatusCode)
            {
                var status = await response.Content.ReadFromJsonAsync<JsonElement>();
                if (status.GetProperty("state").GetString() == expectedState)
                    return status;
            }
            await Task.Delay(200);
        }
        throw new TimeoutException($"Job {jobId} did not reach state '{expectedState}' within {timeout ?? TimeSpan.FromSeconds(10)}");
    }

    private (NotificationWorkerService worker, CancellationTokenSource cts) StartWorker(
        params INotificationSender[] senders)
    {
        var worker = new NotificationWorkerService(
            _app.Multiplexer,
            new SenderRouter(senders),
            new RedisStatusStore(_app.Multiplexer),
            new DeadLetterStore(_app.Multiplexer),
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
        await _app.FlushRedisAsync();
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
        Assert.Equal("Queued", status.GetProperty("state").GetString());
    }

    [Fact]
    public async Task EmailJob_ProcessedByWorker_ReachesDelivered()
    {
        await _app.FlushRedisAsync();
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
            var status = await WaitForStateAsync(jobId, "Delivered");
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
        await _app.FlushRedisAsync();
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
            var status = await WaitForStateAsync(jobId, "DeadLettered", TimeSpan.FromSeconds(25));
            Assert.Equal("DeadLettered", status.GetProperty("state").GetString());
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
        await _app.FlushRedisAsync();
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
            await WaitForStateAsync(originalJobId, "DeadLettered", TimeSpan.FromSeconds(25));
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
        Assert.Equal("Queued", status.GetProperty("state").GetString());
    }

    [Fact]
    public async Task EmailJob_OnceDelivered_SecondWorkerDoesNotReprocess()
    {
        await _app.FlushRedisAsync();

        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "guard@example.com",
            body = "terminal guard test"
        });
        var jobId = await GetJobIdAsync(response);

        // First worker: process the job to Delivered and stop
        var (worker, cts) = StartWorker(new EmailSender(NullLogger<EmailSender>.Instance));
        try
        {
            await WaitForStateAsync(jobId, "Delivered");
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // Second worker with a sender that throws if called:
        // the job is Delivered+ACKed, so the stream entry is gone and
        // the terminal-state guard would also prevent any re-send.
        var faulting = new FaultingSender();
        var (worker2, cts2) = StartWorker(faulting);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2));
            Assert.False(faulting.WasCalled, "FaultingSender was invoked — job was reprocessed after Delivered");
            var statusResponse = await _app.ApiClient.GetAsync($"/notifications/{jobId}");
            var status = await statusResponse.Content.ReadFromJsonAsync<System.Text.Json.JsonElement>();
            Assert.Equal("Delivered", status.GetProperty("state").GetString());
        }
        finally
        {
            cts2.Cancel();
            await worker2.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task DuplicateIdempotencyKey_ReturnsSameJobId_With200()
    {
        await _app.FlushRedisAsync();
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

    [Fact]
    public async Task DlqReplay_CalledTwice_ReturnsSameNewJobId()
    {
        await _app.FlushRedisAsync();

        // Dead-letter a webhook job
        using var mockServer = WireMockServer.Start();
        mockServer.Given(Request.Create().WithPath("/idem-fail").UsingPost())
                  .RespondWith(Response.Create().WithStatusCode(500));

        var response = await PostNotificationAsync(new
        {
            channel = "webhook",
            recipient = $"{mockServer.Url}/idem-fail",
            body = "replay-idempotency test"
        });
        var originalJobId = await GetJobIdAsync(response);

        var webhookSender = new WebhookSender(new HttpClient(), NullLogger<WebhookSender>.Instance);
        var (worker, cts) = StartWorker(webhookSender);
        try
        {
            await WaitForStateAsync(originalJobId, "DeadLettered", TimeSpan.FromSeconds(25));
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // Replay twice
        var replay1 = await _app.ApiClient.PostAsync($"/notifications/dlq/{originalJobId}/replay", null);
        var replay2 = await _app.ApiClient.PostAsync($"/notifications/dlq/{originalJobId}/replay", null);

        Assert.Equal(HttpStatusCode.Accepted, replay1.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, replay2.StatusCode);

        var body1 = await replay1.Content.ReadFromJsonAsync<System.Text.Json.JsonElement>();
        var body2 = await replay2.Content.ReadFromJsonAsync<System.Text.Json.JsonElement>();

        var newJobId1 = body1.GetProperty("newJobId").GetString()!;
        var newJobId2 = body2.GetProperty("newJobId").GetString()!;

        Assert.Equal(newJobId1, newJobId2); // idempotent — same replay produces same new jobId
    }

    private sealed class FaultingSender : INotificationSender
    {
        public bool WasCalled { get; private set; }
        public bool CanHandle(string channel) => true;
        public Task SendAsync(NotificationJob job, CancellationToken cancellationToken = default)
        {
            WasCalled = true;
            throw new InvalidOperationException("FaultingSender: should not be called on terminal jobs");
        }
    }
}
