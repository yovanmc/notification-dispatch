using NotificationDispatch.Core.Models;
using NotificationDispatch.Integration.Fixtures;
using NotificationDispatch.Worker.Services;

namespace NotificationDispatch.Integration;

public class RetryAndDlqTests : IClassFixture<RedisFixture>
{
    private readonly DeadLetterStore _dlq;
    private readonly RetryPolicy _retry;

    public RetryAndDlqTests(RedisFixture redis)
    {
        _dlq = new DeadLetterStore(redis.Connection);
        _retry = new RetryPolicy();
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    [InlineData(3, false)]
    [InlineData(10, false)]
    public void ShouldRetry_RespectsMaxAttempts(int attempt, bool expected)
    {
        Assert.Equal(expected, _retry.ShouldRetry(attempt));
    }

    [Fact]
    public async Task WriteAsync_ThenGetByJobIdAsync_ReturnsEntry()
    {
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
    public async Task GetRecentAsync_ReturnsEntries()
    {
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
        Assert.NotEmpty(recent);
    }
}
