using NotificationDispatch.Api.Services;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;
using NotificationDispatch.Integration.Fixtures;
using NotificationDispatch.Worker.Senders;
using NotificationDispatch.Worker.Services;
using Microsoft.Extensions.Logging.Abstractions;

namespace NotificationDispatch.Integration;

public class NotificationLifecycleTests : IClassFixture<RedisFixture>
{
    private readonly RedisFixture _redis;
    private readonly RedisStreamProducer _producer;
    private readonly RedisStatusStore _statusStore;
    private readonly DeadLetterStore _dlq;

    public NotificationLifecycleTests(RedisFixture redis)
    {
        _redis = redis;
        _producer = new RedisStreamProducer(redis.Connection);
        _statusStore = new RedisStatusStore(redis.Connection);
        _dlq = new DeadLetterStore(redis.Connection);
    }

    [Fact]
    public async Task FullLifecycle_EmailJob_EndsDelivered()
    {
        // Arrange
        var request = new NotificationRequest
        {
            Channel = "email",
            Recipient = "lifecycle@test.com",
            Body = "Lifecycle test"
        };

        // Act — enqueue
        var (created, jobId) = await _producer.EnqueueAsync(request, Guid.NewGuid().ToString());
        Assert.True(created);

        // Verify queued status
        var status = await _statusStore.GetAsync(jobId);
        Assert.NotNull(status);
        Assert.Equal(DeliveryState.Queued, status.State);

        // Simulate worker processing
        var senders = new INotificationSender[]
        {
            new EmailSender(NullLogger<EmailSender>.Instance)
        };
        var router = new SenderRouter(senders);

        var job = new NotificationJob
        {
            JobId = jobId,
            Request = request,
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        const int attemptCount = 1;
        await _statusStore.UpdateStateAsync(jobId, DeliveryState.Processing, attemptCount);
        await router.RouteAsync(job, TestContext.Current.CancellationToken);
        await _statusStore.UpdateStateAsync(jobId, DeliveryState.Delivered, attemptCount);

        // Assert — delivered
        var final = await _statusStore.GetAsync(jobId);
        Assert.NotNull(final);
        Assert.Equal(DeliveryState.Delivered, final.State);
        Assert.Equal(attemptCount, final.Attempts);
        Assert.NotNull(final.CompletedAt);
    }
}
