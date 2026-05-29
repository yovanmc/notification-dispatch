using NotificationDispatch.Api.Services;
using NotificationDispatch.Core.Models;
using NotificationDispatch.Integration.Fixtures;
using NotificationDispatch.Worker.Services;

namespace NotificationDispatch.Integration;

public class DlqReplayTests : IClassFixture<RedisFixture>
{
    private readonly RedisStreamProducer _producer;
    private readonly RedisStatusStore _statusStore;
    private readonly DeadLetterStore _dlq;

    public DlqReplayTests(RedisFixture redis)
    {
        _producer = new RedisStreamProducer(redis.Connection);
        _statusStore = new RedisStatusStore(redis.Connection);
        _dlq = new DeadLetterStore(redis.Connection);
    }

    [Fact]
    public async Task Replay_CreatesNewQueuedJob()
    {
        // Arrange — create a dead-lettered job
        var request = new NotificationRequest
        {
            Channel = "webhook",
            Recipient = "https://example.com/fail",
            Body = "Will fail"
        };

        var (_, originalJobId) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());

        var originalJob = new NotificationJob
        {
            JobId = originalJobId,
            Request = request,
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        await _statusStore.UpdateStateAsync(originalJobId, DeliveryState.DeadLettered, 3, "Connection refused");
        await _dlq.WriteAsync(originalJob, 3, "Connection refused",
            DateTimeOffset.UtcNow.AddSeconds(-6), DateTimeOffset.UtcNow);

        // Act — replay the DLQ entry
        var dlqEntry = await _dlq.GetByJobIdAsync(originalJobId);
        Assert.NotNull(dlqEntry);

        var replayedJob = NotificationJob.FromJson(dlqEntry["payload"]);
        var replayKey = $"replay-{originalJobId}-{DateTimeOffset.UtcNow.Ticks}";
        var (replayCreated, replayJobId) = await _producer.EnqueueAsync(replayedJob.Request, replayKey);

        // Assert
        Assert.True(replayCreated);
        Assert.NotEqual(originalJobId, replayJobId);

        var replayStatus = await _statusStore.GetAsync(replayJobId);
        Assert.NotNull(replayStatus);
        Assert.Equal(DeliveryState.Queued, replayStatus.State);
    }
}
