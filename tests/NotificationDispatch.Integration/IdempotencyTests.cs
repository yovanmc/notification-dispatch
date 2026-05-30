using NotificationDispatch.Core.Models;
using NotificationDispatch.Infrastructure;
using NotificationDispatch.Integration.Fixtures;

namespace NotificationDispatch.Integration;

public class IdempotencyTests : IClassFixture<RedisFixture>
{
    private readonly RedisStreamProducer _producer;
    private readonly RedisStatusStore _statusStore;

    public IdempotencyTests(RedisFixture redis)
    {
        _producer = new RedisStreamProducer(redis.Connection);
        _statusStore = new RedisStatusStore(redis.Connection);
    }

    [Fact]
    public async Task EnqueueAsync_FirstCall_CreatesJobAndReturnsCreatedTrue()
    {
        var request = new NotificationRequest
        {
            Channel = "email",
            Recipient = "test@example.com",
            Body = "Hello"
        };

        var (created, jobId) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());

        Assert.True(created);
        Assert.False(string.IsNullOrEmpty(jobId));

        var status = await _statusStore.GetAsync(jobId);
        Assert.NotNull(status);
        Assert.Equal(DeliveryState.Queued, status.State);
    }

    [Fact]
    public async Task EnqueueAsync_DuplicateKey_ReturnsSameJobIdAndCreatedFalse()
    {
        var idempotencyKey = Guid.NewGuid().ToString();
        var request = new NotificationRequest
        {
            Channel = "sms",
            Recipient = "+15551234567",
            Body = "Duplicate test"
        };

        var (created1, jobId1) = await _producer.EnqueueAsync(request, idempotencyKey);
        var (created2, jobId2) = await _producer.EnqueueAsync(request, idempotencyKey);

        Assert.True(created1);
        Assert.False(created2);
        Assert.Equal(jobId1, jobId2);
    }

    [Fact]
    public async Task EnqueueAsync_DifferentKeys_CreatesDifferentJobs()
    {
        var request = new NotificationRequest
        {
            Channel = "webhook",
            Recipient = "https://example.com/hook",
            Body = "{\"event\":\"test\"}"
        };

        var (created1, jobId1) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());
        var (created2, jobId2) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());

        Assert.True(created1);
        Assert.True(created2);
        Assert.NotEqual(jobId1, jobId2);
    }
}
