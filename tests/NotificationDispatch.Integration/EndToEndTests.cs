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

    private async Task<HttpResponseMessage> PostNotificationAsync(
        object body, string? idempotencyKey = null, CancellationToken cancellationToken = default)
    {
        idempotencyKey ??= Guid.NewGuid().ToString();
        var request = new HttpRequestMessage(HttpMethod.Post, "/notifications")
        {
            Content = JsonContent.Create(body)
        };
        request.Headers.Add("Idempotency-Key", idempotencyKey);
        return await _app.ApiClient.SendAsync(request, cancellationToken);
    }

    private static async Task<string> GetJobIdAsync(
        HttpResponseMessage response, CancellationToken cancellationToken = default)
    {
        var body = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken);
        return body.GetProperty("jobId").GetString()!;
    }

    private async Task<JsonElement> WaitForStateAsync(string jobId, string expectedState,
        TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTimeOffset.UtcNow < deadline)
        {
            var response = await _app.ApiClient.GetAsync($"/notifications/{jobId}", cancellationToken);
            if (response.IsSuccessStatusCode)
            {
                var status = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken);
                if (status.GetProperty("state").GetString() == expectedState)
                    return status;
            }
            await Task.Delay(200, cancellationToken);
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
        var ct = TestContext.Current.CancellationToken;
        await _app.FlushRedisAsync();
        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "e2e@example.com",
            body = "scenario 1"
        }, cancellationToken: ct);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var jobId = await GetJobIdAsync(response, ct);
        Assert.False(string.IsNullOrEmpty(jobId));

        var statusResponse = await _app.ApiClient.GetAsync($"/notifications/{jobId}", ct);
        Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
        var status = await statusResponse.Content.ReadFromJsonAsync<JsonElement>(ct);
        Assert.Equal("Queued", status.GetProperty("state").GetString());
    }

    [Fact]
    public async Task EmailJob_ProcessedByWorker_ReachesDelivered()
    {
        var ct = TestContext.Current.CancellationToken;
        await _app.FlushRedisAsync();
        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "delivered@example.com",
            body = "scenario 2"
        }, cancellationToken: ct);
        var jobId = await GetJobIdAsync(response, ct);

        var (worker, cts) = StartWorker(new EmailSender(NullLogger<EmailSender>.Instance));
        try
        {
            var status = await WaitForStateAsync(jobId, "Delivered", cancellationToken: ct);
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
        var ct = TestContext.Current.CancellationToken;
        await _app.FlushRedisAsync();
        using var mockServer = WireMockServer.Start();
        mockServer.Given(Request.Create().WithPath("/fail").UsingPost())
                  .RespondWith(Response.Create().WithStatusCode(500).WithBody("internal error"));

        var response = await PostNotificationAsync(new
        {
            channel = "webhook",
            recipient = $"{mockServer.Url}/fail",
            body = "scenario 3"
        }, cancellationToken: ct);
        var jobId = await GetJobIdAsync(response, ct);

        var webhookSender = new WebhookSender(new HttpClient(), NullLogger<WebhookSender>.Instance);
        var (worker, cts) = StartWorker(webhookSender);
        try
        {
            // 3 retries × up to 4s each = ~12s worst case; give 25s
            var status = await WaitForStateAsync(jobId, "DeadLettered", TimeSpan.FromSeconds(25), ct);
            Assert.Equal("DeadLettered", status.GetProperty("state").GetString());
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // DLQ entry should exist via the API
        var dlqResponse = await _app.ApiClient.GetAsync("/notifications/dlq?count=50", ct);
        Assert.Equal(HttpStatusCode.OK, dlqResponse.StatusCode);
        var entries = await dlqResponse.Content.ReadFromJsonAsync<JsonElement[]>(ct);
        Assert.NotNull(entries);
        Assert.Contains(entries, e =>
            e.TryGetProperty("jobId", out var id) && id.GetString() == jobId);
    }

    [Fact]
    public async Task DlqReplay_ViaApi_CreatesNewQueuedJob()
    {
        var ct = TestContext.Current.CancellationToken;
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
        }, cancellationToken: ct);
        var originalJobId = await GetJobIdAsync(response, ct);

        var webhookSender = new WebhookSender(new HttpClient(), NullLogger<WebhookSender>.Instance);
        var (worker, cts) = StartWorker(webhookSender);
        try
        {
            await WaitForStateAsync(originalJobId, "DeadLettered", TimeSpan.FromSeconds(25), ct);
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // Act: call replay endpoint
        var replayResponse = await _app.ApiClient.PostAsync(
            $"/notifications/dlq/{originalJobId}/replay", null, ct);

        Assert.Equal(HttpStatusCode.Accepted, replayResponse.StatusCode);
        var replayBody = await replayResponse.Content.ReadFromJsonAsync<JsonElement>(ct);
        var newJobId = replayBody.GetProperty("newJobId").GetString()!;
        Assert.NotEqual(originalJobId, newJobId);

        // Assert: new job is Queued (not yet processed)
        var statusResponse = await _app.ApiClient.GetAsync($"/notifications/{newJobId}", ct);
        Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
        var status = await statusResponse.Content.ReadFromJsonAsync<JsonElement>(ct);
        Assert.Equal("Queued", status.GetProperty("state").GetString());
    }

    [Fact]
    public async Task EmailJob_OnceDelivered_SecondWorkerDoesNotReprocess()
    {
        var ct = TestContext.Current.CancellationToken;
        await _app.FlushRedisAsync();

        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "guard@example.com",
            body = "terminal guard test"
        }, cancellationToken: ct);
        var jobId = await GetJobIdAsync(response, ct);

        // First worker: process the job to Delivered and stop
        var (worker, cts) = StartWorker(new EmailSender(NullLogger<EmailSender>.Instance));
        try
        {
            await WaitForStateAsync(jobId, "Delivered", cancellationToken: ct);
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
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
            Assert.False(faulting.WasCalled, "FaultingSender was invoked — job was reprocessed after Delivered");
            var statusResponse = await _app.ApiClient.GetAsync($"/notifications/{jobId}", ct);
            var status = await statusResponse.Content.ReadFromJsonAsync<JsonElement>(ct);
            Assert.Equal("Delivered", status.GetProperty("state").GetString());
        }
        finally
        {
            cts2.Cancel();
            await worker2.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task TerminalStateGuard_UnconsumedEntryWithDeliveredStatus_SkipsResendAndAcks()
    {
        // Exercises the terminal-state guard: status is set to Delivered before the stream
        // entry is consumed. When the worker reads the entry from ">", the guard detects
        // Delivered status and ACKs without routing to any sender.
        //
        // This covers the same guard code path as crash-after-send-before-ACK (where the
        // entry is in the PEL), but simulates it via a ">" entry with pre-set terminal status.
        // The Docker Compose smoke test exercises end-to-end delivery including DLQ/replay;
        // a true PEL/XAUTOCLAIM reclaim test would require coordinating consumer ownership
        // and the 150s idle threshold, which is impractical in a unit/integration test.
        var ct = TestContext.Current.CancellationToken;
        await _app.FlushRedisAsync();

        var response = await PostNotificationAsync(new
        {
            channel = "email",
            recipient = "guard-reclaim@example.com",
            body = "terminal guard reclaim test"
        }, cancellationToken: ct);
        var jobId = await GetJobIdAsync(response, ct);

        // Directly mark status Delivered without letting a worker ACK the stream entry.
        var statusStore = new RedisStatusStore(_app.Multiplexer);
        await statusStore.UpdateStateAsync(jobId, DeliveryState.Delivered, 1);

        // Start a worker that throws if the sender is ever called.
        // The terminal-state guard should detect Delivered status and ACK without routing.
        var faulting = new FaultingSender();
        var (worker, cts) = StartWorker(faulting);
        try
        {
            // Give the worker time to consume the stream entry and execute the guard.
            await Task.Delay(TimeSpan.FromSeconds(3), ct);

            Assert.False(faulting.WasCalled,
                "Terminal-state guard failed: sender was invoked on a job already in Delivered state");

            // Stream entry must have been ACKed — a second worker sees nothing to process.
            var faulting2 = new FaultingSender();
            var (worker2, cts2) = StartWorker(faulting2);
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
                Assert.False(faulting2.WasCalled, "Stream entry was not ACKed by the terminal-state guard");
            }
            finally
            {
                cts2.Cancel();
                await worker2.StopAsync(CancellationToken.None);
            }
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task DuplicateIdempotencyKey_ReturnsSameJobId_With200()
    {
        var ct = TestContext.Current.CancellationToken;
        await _app.FlushRedisAsync();
        var key = Guid.NewGuid().ToString();
        var body = new { channel = "email", recipient = "idem@example.com", body = "scenario 5" };

        var first = await PostNotificationAsync(body, key, ct);
        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        var firstJobId = await GetJobIdAsync(first, ct);

        var second = await PostNotificationAsync(body, key, ct);
        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        var secondBody = await second.Content.ReadFromJsonAsync<JsonElement>(ct);
        var secondJobId = secondBody.GetProperty("jobId").GetString()!;

        Assert.Equal(firstJobId, secondJobId);
    }

    [Fact]
    public async Task DlqReplay_CalledTwice_ReturnsSameNewJobId()
    {
        var ct = TestContext.Current.CancellationToken;
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
        }, cancellationToken: ct);
        var originalJobId = await GetJobIdAsync(response, ct);

        var webhookSender = new WebhookSender(new HttpClient(), NullLogger<WebhookSender>.Instance);
        var (worker, cts) = StartWorker(webhookSender);
        try
        {
            await WaitForStateAsync(originalJobId, "DeadLettered", TimeSpan.FromSeconds(25), ct);
        }
        finally
        {
            cts.Cancel();
            await worker.StopAsync(CancellationToken.None);
        }

        // Replay twice
        var replay1 = await _app.ApiClient.PostAsync($"/notifications/dlq/{originalJobId}/replay", null, ct);
        var replay2 = await _app.ApiClient.PostAsync($"/notifications/dlq/{originalJobId}/replay", null, ct);

        Assert.Equal(HttpStatusCode.Accepted, replay1.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, replay2.StatusCode);

        var body1 = await replay1.Content.ReadFromJsonAsync<JsonElement>(ct);
        var body2 = await replay2.Content.ReadFromJsonAsync<JsonElement>(ct);

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
