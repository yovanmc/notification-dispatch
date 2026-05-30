using NotificationDispatch.Core.Models;
using NotificationDispatch.Infrastructure;
using NotificationDispatch.Integration.Fixtures;

namespace NotificationDispatch.Integration;

[Collection("Integration")]
public class IdempotencyTests
{
    private readonly RedisFixture _redis;
    private readonly RedisStreamProducer _producer;
    private readonly RedisStatusStore _statusStore;

    public IdempotencyTests(RedisFixture redis)
    {
        _redis = redis;
        _producer = new RedisStreamProducer(redis.Connection);
        _statusStore = new RedisStatusStore(redis.Connection);
    }

    [Fact]
    public async Task EnqueueAsync_FirstCall_CreatesJobAndReturnsCreatedTrue()
    {
        await _redis.FlushAsync();

        var request = new NotificationRequest
        {
            Channel = "email",
            Recipient = "test@example.com",
            Body = "Hello"
        };

        var (result, jobId) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());

        Assert.Equal(EnqueueResult.Created, result);
        Assert.False(string.IsNullOrEmpty(jobId));

        var status = await _statusStore.GetAsync(jobId);
        Assert.NotNull(status);
        Assert.Equal(DeliveryState.Queued, status.State);
    }

    [Fact]
    public async Task EnqueueAsync_DuplicateKey_ReturnsSameJobIdAndDuplicateResult()
    {
        await _redis.FlushAsync();

        var idempotencyKey = Guid.NewGuid().ToString();
        var request = new NotificationRequest
        {
            Channel = "sms",
            Recipient = "+15551234567",
            Body = "Duplicate test"
        };

        var (result1, jobId1) = await _producer.EnqueueAsync(request, idempotencyKey);
        var (result2, jobId2) = await _producer.EnqueueAsync(request, idempotencyKey);

        Assert.Equal(EnqueueResult.Created, result1);
        Assert.Equal(EnqueueResult.Duplicate, result2);
        Assert.Equal(jobId1, jobId2);
    }

    [Fact]
    public async Task EnqueueAsync_DifferentKeys_CreatesDifferentJobs()
    {
        await _redis.FlushAsync();

        var request = new NotificationRequest
        {
            Channel = "webhook",
            Recipient = "https://example.com/hook",
            Body = "{\"event\":\"test\"}"
        };

        var (result1, jobId1) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());
        var (result2, jobId2) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());

        Assert.Equal(EnqueueResult.Created, result1);
        Assert.Equal(EnqueueResult.Created, result2);
        Assert.NotEqual(jobId1, jobId2);
    }

    [Fact]
    public async Task EnqueueAsync_SameKeyDifferentBody_ReturnsConflict()
    {
        await _redis.FlushAsync();

        var idempotencyKey = Guid.NewGuid().ToString();
        var request1 = new NotificationRequest
        {
            Channel = "email",
            Recipient = "test@example.com",
            Body = "Original body"
        };
        var request2 = request1 with { Body = "Different body" };

        var (result1, jobId1) = await _producer.EnqueueAsync(request1, idempotencyKey);
        var (result2, conflictJobId) = await _producer.EnqueueAsync(request2, idempotencyKey);

        Assert.Equal(EnqueueResult.Created, result1);
        Assert.Equal(EnqueueResult.Conflict, result2);
        Assert.Equal(jobId1, conflictJobId); // conflict returns the original job id, not a new one
    }

    [Fact]
    public async Task EnqueueAsync_SameKeyDifferentMetadata_ReturnsConflict()
    {
        await _redis.FlushAsync();

        var idempotencyKey = Guid.NewGuid().ToString();
        var request1 = new NotificationRequest
        {
            Channel = "email",
            Recipient = "meta@example.com",
            Body = "metadata conflict test",
            Metadata = new Dictionary<string, string> { ["priority"] = "high" }
        };
        var request2 = request1 with
        {
            Metadata = new Dictionary<string, string> { ["priority"] = "low" }
        };

        var (result1, _) = await _producer.EnqueueAsync(request1, idempotencyKey);
        var (result2, _) = await _producer.EnqueueAsync(request2, idempotencyKey);

        Assert.Equal(EnqueueResult.Created, result1);
        Assert.Equal(EnqueueResult.Conflict, result2);
    }

    [Fact]
    public async Task EnqueueAsync_SameKeyMetadataDifferentInsertionOrder_ReturnsDuplicate()
    {
        await _redis.FlushAsync();

        // Metadata with the same keys/values but different insertion order must hash identically.
        var idempotencyKey = Guid.NewGuid().ToString();
        var request1 = new NotificationRequest
        {
            Channel = "email",
            Recipient = "meta@example.com",
            Body = "metadata ordering test",
            Metadata = new Dictionary<string, string> { ["z-key"] = "1", ["a-key"] = "2" }
        };
        var request2 = new NotificationRequest
        {
            Channel = "email",
            Recipient = "meta@example.com",
            Body = "metadata ordering test",
            Metadata = new Dictionary<string, string> { ["a-key"] = "2", ["z-key"] = "1" }
        };

        var (result1, jobId1) = await _producer.EnqueueAsync(request1, idempotencyKey);
        var (result2, jobId2) = await _producer.EnqueueAsync(request2, idempotencyKey);

        Assert.Equal(EnqueueResult.Created, result1);
        Assert.Equal(EnqueueResult.Duplicate, result2);
        Assert.Equal(jobId1, jobId2);
    }
}
