using NotificationDispatch.Core.Models;
using StackExchange.Redis;

namespace NotificationDispatch.Infrastructure;

public class DeadLetterStore
{
    private readonly IConnectionMultiplexer _redis;
    private const string DlqStream = RedisConstants.DlqStreamKey;

    public DeadLetterStore(IConnectionMultiplexer redis)
    {
        _redis = redis;
    }

    public async Task WriteAsync(NotificationJob job, int attemptCount, string lastError,
        DateTimeOffset firstFailureTime, DateTimeOffset finalFailureTime)
    {
        var db = _redis.GetDatabase();
        var entry = new NameValueEntry[]
        {
            new("jobId", job.JobId),
            new("payload", job.ToJson()),
            new("attemptCount", attemptCount),
            new("lastError", lastError),
            new("firstFailureTime", firstFailureTime.ToString("O")),
            new("finalFailureTime", finalFailureTime.ToString("O"))
        };

        await db.StreamAddAsync(DlqStream, entry, maxLength: 10_000, useApproximateMaxLength: true);
    }

    public async Task WriteMalformedAsync(string streamEntryId, string rawPayload, string parseError)
    {
        var db = _redis.GetDatabase();
        await db.StreamAddAsync(DlqStream, new NameValueEntry[]
        {
            new("streamEntryId", streamEntryId),
            new("rawPayload", rawPayload.Length > 1000 ? rawPayload[..1000] + "…" : rawPayload),
            new("parseError", parseError),
            new("timestamp", DateTimeOffset.UtcNow.ToString("O")),
            new("type", "malformed")
        }, maxLength: 10_000, useApproximateMaxLength: true);
    }

    public async Task<List<Dictionary<string, string>>> GetRecentAsync(int count = 20)
    {
        var db = _redis.GetDatabase();
        var entries = await db.StreamRangeAsync(DlqStream, count: count, messageOrder: Order.Descending);

        return entries.Select(e => e.Values.ToDictionary(
            v => v.Name.ToString(),
            v => v.Value.ToString()
        )).ToList();
    }

    // Linear scan — acceptable for DLQ volumes. Returns the first (oldest) match.
    public async Task<Dictionary<string, string>?> GetByJobIdAsync(string jobId)
    {
        var db = _redis.GetDatabase();
        var entries = await db.StreamRangeAsync(DlqStream, count: 10_000);

        var match = entries.FirstOrDefault(e =>
            e.Values.Any(v => v.Name == "jobId" && v.Value == jobId));

        if (match.IsNull) return null;

        return match.Values.ToDictionary(
            v => v.Name.ToString(),
            v => v.Value.ToString()
        );
    }
}
