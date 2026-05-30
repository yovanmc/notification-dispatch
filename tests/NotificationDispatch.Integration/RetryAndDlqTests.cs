using NotificationDispatch.Core.Models;
using NotificationDispatch.Infrastructure;
using NotificationDispatch.Integration.Fixtures;

namespace NotificationDispatch.Integration;

[Collection("Integration")]
public class RetryAndDlqTests
{
    private readonly DeadLetterStore _dlq;
    private readonly RedisFixture _redis;

    public RetryAndDlqTests(RedisFixture redis)
    {
        _redis = redis;
        _dlq = new DeadLetterStore(redis.Connection);
    }

    [Fact]
    public async Task WriteAsync_ThenGetByJobIdAsync_ReturnsEntry()
    {
        await _redis.FlushAsync();

        var job = new NotificationJob
        {
            JobId = Guid.NewGuid().ToString(),
            Request = new NotificationRequest
            {
                Channel = "webhook",
                Recipient = "https://example.com/fail",
                Body = "test"
            },
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        var now = DateTimeOffset.UtcNow;
        await _dlq.WriteAsync(job, 3, "Connection refused", now.AddSeconds(-6), now);

        var entry = await _dlq.GetByJobIdAsync(job.JobId);
        Assert.NotNull(entry);
        Assert.Equal(job.JobId, entry["jobId"]);
        Assert.Equal("3", entry["attemptCount"]);
        Assert.Equal("Connection refused", entry["lastError"]);
    }

    [Fact]
    public async Task GetRecentAsync_ReturnsOnlyThisTestEntry()
    {
        await _redis.FlushAsync();

        var job = new NotificationJob
        {
            JobId = Guid.NewGuid().ToString(),
            Request = new NotificationRequest
            {
                Channel = "email",
                Recipient = "fail@test.com",
                Body = "DLQ test"
            },
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        await _dlq.WriteAsync(job, 3, "Timeout", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow);

        var recent = await _dlq.GetRecentAsync(5);
        // After flush, only our entry is present
        Assert.Single(recent);
        Assert.Contains(recent, e => e.TryGetValue("jobId", out var id) && id == job.JobId);
    }
}
