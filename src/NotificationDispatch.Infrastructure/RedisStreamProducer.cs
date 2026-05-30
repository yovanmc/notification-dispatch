using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using NotificationDispatch.Core.Models;
using StackExchange.Redis;

namespace NotificationDispatch.Infrastructure;

public class RedisStreamProducer
{
    private readonly IConnectionMultiplexer _redis;
    private readonly string _luaScript;

    private const string StreamKey = RedisConstants.JobStreamKey;
    private const string ConsumerGroup = RedisConstants.ConsumerGroupName;
    private const int IdempotencyTtlSeconds = 86400; // 24 hours
    private const int StatusTtlSeconds = 604800; // 7 days — passed to enqueue.lua for initial status write

    public RedisStreamProducer(IConnectionMultiplexer redis)
    {
        _redis = redis;
        _luaScript = LoadLuaScript();
        EnsureConsumerGroup();
    }

    public async Task<(EnqueueResult Result, string JobId)> EnqueueAsync(
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

        var requestHash = ComputeRequestHash(request);

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
            StatusTtlSeconds,
            requestHash
        };

        var result = (RedisResult[]?)await db.ScriptEvaluateAsync(_luaScript, keys, args);
        if (result is null)
            throw new InvalidOperationException("Lua script returned null");

        var code = (long)result[0];
        var returnedJobId = (string)result[1]!;

        return code switch
        {
            0 => (EnqueueResult.Duplicate, returnedJobId),
            1 => (EnqueueResult.Created, returnedJobId),
            2 => (EnqueueResult.Conflict, returnedJobId),
            _ => throw new InvalidOperationException($"Unexpected Lua result code: {code}")
        };
    }

    private static string ComputeRequestHash(NotificationRequest request)
    {
        // Canonical form: channel:recipient:body (metadata excluded for simplicity)
        var canonical = $"{request.Channel}:{request.Recipient}:{request.Body}";
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexString(bytes).ToLowerInvariant();
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
            .FirstOrDefault(n => n.EndsWith("enqueue.lua"))
            ?? throw new InvalidOperationException(
                "Embedded resource 'enqueue.lua' not found. Verify it is declared as EmbeddedResource in the .csproj.");
        using var stream = assembly.GetManifestResourceStream(resourceName)!;
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
