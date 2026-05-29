using System.Text.Json;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;
using StackExchange.Redis;

namespace NotificationDispatch.Worker.Services;

public class RedisStatusStore : IStatusStore
{
    private readonly IConnectionMultiplexer _redis;
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(24);

    public RedisStatusStore(IConnectionMultiplexer redis)
    {
        _redis = redis;
    }

    public async Task SetAsync(NotificationStatus status)
    {
        var db = _redis.GetDatabase();
        var key = $"notification:status:{status.JobId}";
        var json = JsonSerializer.Serialize(status);
        await db.StringSetAsync(key, json, Ttl);
    }

    public async Task<NotificationStatus?> GetAsync(string jobId)
    {
        var db = _redis.GetDatabase();
        var key = $"notification:status:{jobId}";
        var json = await db.StringGetAsync(key);
        if (json.IsNullOrEmpty) return null;
        return JsonSerializer.Deserialize<NotificationStatus>((string)json!);
    }

    // Note: read-modify-write is non-atomic. Safe for the current single-consumer-per-job
    // Worker design, but not safe for concurrent updates to the same jobId.
    public async Task UpdateStateAsync(string jobId, DeliveryState state, int attempts, string? lastError = null)
    {
        var existing = await GetAsync(jobId);
        if (existing is null) throw new InvalidOperationException($"Cannot update state for job '{jobId}': status record not found.");

        var updated = existing with
        {
            State = state,
            Attempts = attempts,
            LastError = lastError,
            CompletedAt = state is DeliveryState.Delivered or DeliveryState.DeadLettered
                ? DateTimeOffset.UtcNow
                : null
        };

        await SetAsync(updated);
    }
}
