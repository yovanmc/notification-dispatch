using NotificationDispatch.Core.Models;
using NotificationDispatch.Integration.Fixtures;
using NotificationDispatch.Worker.Services;

namespace NotificationDispatch.Integration;

public class StatusStoreTests : IClassFixture<RedisFixture>
{
    private readonly RedisStatusStore _store;

    public StatusStoreTests(RedisFixture redis)
    {
        _store = new RedisStatusStore(redis.Connection);
    }

    [Fact]
    public async Task SetAsync_ThenGetAsync_ReturnsStoredStatus()
    {
        var status = new NotificationStatus
        {
            JobId = Guid.NewGuid().ToString(),
            Channel = "email",
            State = DeliveryState.Queued,
            Attempts = 0
        };

        await _store.SetAsync(status);
        var result = await _store.GetAsync(status.JobId);

        Assert.NotNull(result);
        Assert.Equal(status.JobId, result.JobId);
        Assert.Equal(DeliveryState.Queued, result.State);
        Assert.Equal("email", result.Channel);
    }

    [Fact]
    public async Task GetAsync_NonExistent_ReturnsNull()
    {
        var result = await _store.GetAsync("does-not-exist");
        Assert.Null(result);
    }

    [Fact]
    public async Task UpdateStateAsync_ChangesStateAndAttempts()
    {
        var jobId = Guid.NewGuid().ToString();
        var status = new NotificationStatus
        {
            JobId = jobId,
            Channel = "sms",
            State = DeliveryState.Queued,
            Attempts = 0
        };

        await _store.SetAsync(status);
        await _store.UpdateStateAsync(jobId, DeliveryState.Processing, 1);

        var result = await _store.GetAsync(jobId);
        Assert.NotNull(result);
        Assert.Equal(DeliveryState.Processing, result.State);
        Assert.Equal(1, result.Attempts);
    }

    [Fact]
    public async Task UpdateStateAsync_WithError_StoresError()
    {
        var jobId = Guid.NewGuid().ToString();
        var status = new NotificationStatus
        {
            JobId = jobId,
            Channel = "webhook",
            State = DeliveryState.Queued,
            Attempts = 0
        };

        await _store.SetAsync(status);
        await _store.UpdateStateAsync(jobId, DeliveryState.DeadLettered, 3, "Connection refused");

        var result = await _store.GetAsync(jobId);
        Assert.NotNull(result);
        Assert.Equal(DeliveryState.DeadLettered, result.State);
        Assert.Equal(3, result.Attempts);
        Assert.Equal("Connection refused", result.LastError);
    }
}
