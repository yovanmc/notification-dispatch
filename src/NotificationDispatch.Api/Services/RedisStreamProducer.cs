using System.Reflection;
using System.Text.Json;
using NotificationDispatch.Core.Models;
using StackExchange.Redis;

namespace NotificationDispatch.Api.Services;

public class RedisStreamProducer
{
    private readonly IConnectionMultiplexer _redis;
    private readonly string _luaScript;

    private const string StreamKey = "notifications:jobs";
    private const string ConsumerGroup = "worker-group";
    private const int IdempotencyTtlSeconds = 86400; // 24 hours
    private const int StatusTtlSeconds = 86400;

    public RedisStreamProducer(IConnectionMultiplexer redis)
    {
        _redis = redis;
        _luaScript = LoadLuaScript();
        EnsureConsumerGroup();
    }

    public async Task<(bool Created, string JobId)> EnqueueAsync(
        NotificationRequest request, string idempotencyKey)
    {
        var db = _redis.GetDatabase();
        var jobId = Guid.NewGuid().ToString();

        var job = new NotificationJob
        {
            JobId = jobId,
            Request = request,
            EnqueuedAt = DateTimeOffset.UtcNow
        };

        var initialStatus = new NotificationStatus
        {
            JobId = jobId,
            Channel = request.Channel,
            State = DeliveryState.Queued,
            Attempts = 0
        };

        var keys = new RedisKey[]
        {
            $"notification:idempotency:{idempotencyKey}",
            $"notification:status:{jobId}",
            StreamKey
        };

        var args = new RedisValue[]
        {
            jobId,
            job.ToJson(),
            JsonSerializer.Serialize(initialStatus),
            IdempotencyTtlSeconds,
            StatusTtlSeconds
        };

        var result = (RedisResult[]?)await db.ScriptEvaluateAsync(_luaScript, keys, args);
        if (result is null)
            throw new InvalidOperationException("Lua script returned null");

        var created = (long)result[0] == 1;
        var returnedJobId = (string)result[1]!;

        return (created, returnedJobId);
    }

    private void EnsureConsumerGroup()
    {
        var db = _redis.GetDatabase();
        try
        {
            db.StreamCreateConsumerGroup(StreamKey, ConsumerGroup, "0-0", createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Consumer group already exists — expected on restart
        }
    }

    private static string LoadLuaScript()
    {
        var assembly = Assembly.GetExecutingAssembly();
        var resourceName = assembly.GetManifestResourceNames()
            .First(n => n.EndsWith("enqueue.lua"));
        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
