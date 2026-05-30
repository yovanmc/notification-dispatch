using NotificationDispatch.Core.Models;
using NotificationDispatch.Infrastructure;
using StackExchange.Redis;

namespace NotificationDispatch.Worker.Services;

public class NotificationWorkerService : BackgroundService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly SenderRouter _router;
    private readonly RedisStatusStore _statusStore;
    private readonly DeadLetterStore _dlq;
    private readonly RetryPolicy _retryPolicy;
    private readonly ILogger<NotificationWorkerService> _logger;

    private const string StreamKey = RedisConstants.JobStreamKey;
    private const string ConsumerGroup = RedisConstants.ConsumerGroupName;
    private readonly string _consumerId = $"worker-{Environment.MachineName}-{Guid.NewGuid():N}";

    public NotificationWorkerService(
        IConnectionMultiplexer redis,
        SenderRouter router,
        RedisStatusStore statusStore,
        DeadLetterStore dlq,
        RetryPolicy retryPolicy,
        ILogger<NotificationWorkerService> logger)
    {
        _redis = redis;
        _router = router;
        _statusStore = statusStore;
        _dlq = dlq;
        _retryPolicy = retryPolicy;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var db = _redis.GetDatabase();
        _logger.LogInformation("Worker {ConsumerId} starting on stream {Stream}",
            _consumerId, StreamKey);

        await EnsureConsumerGroupAsync(db);
        await ReclaimPendingEntriesAsync(db, stoppingToken);

        var lastReclaimAt = DateTimeOffset.UtcNow;
        const int reclaimIntervalSeconds = 30;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if ((DateTimeOffset.UtcNow - lastReclaimAt).TotalSeconds >= reclaimIntervalSeconds)
                {
                    await ReclaimPendingEntriesAsync(db, stoppingToken);
                    lastReclaimAt = DateTimeOffset.UtcNow;
                }

                var entries = await db.StreamReadGroupAsync(
                    StreamKey, ConsumerGroup, _consumerId,
                    ">", count: 1);

                if (entries.Length == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);
                    continue;
                }

                var entry = entries[0];
                var payload = ExtractPayload(entry.Values);
                if (payload is null)
                {
                    _logger.LogError(
                        "Stream entry {EntryId} is missing 'payload' field — writing to DLQ and ACKing",
                        entry.Id);
                    await WriteMalformedAndAckAsync(db, entry.Id, "(missing payload field)",
                        "payload field absent", source: "live");
                    continue;
                }

                NotificationJob job;
                try
                {
                    job = NotificationJob.FromJson(payload);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Malformed payload in stream entry {EntryId} — writing to DLQ and ACKing", entry.Id);
                    await WriteMalformedAndAckAsync(db, entry.Id, payload, ex.Message, source: "live");
                    continue;
                }

                _logger.LogInformation("Processing job {JobId} on channel {Channel}",
                    job.JobId, job.Request.Channel);

                await ProcessJobAsync(db, entry.Id, job, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in worker loop");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }

        _logger.LogInformation("Worker {ConsumerId} stopping", _consumerId);
    }

    private async Task EnsureConsumerGroupAsync(IDatabase db)
    {
        try
        {
            await db.StreamCreateConsumerGroupAsync(StreamKey, ConsumerGroup, "0", createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP"))
        {
            // Group already exists — normal on restart
        }
    }

    // On startup, reclaim any entries that were mid-flight when a previous worker instance crashed.
    // XAUTOCLAIM returns entries that have been pending longer than the idle threshold.
    private async Task ReclaimPendingEntriesAsync(IDatabase db, CancellationToken stoppingToken)
    {
        // Worst-case processing window: 30s HTTP timeout × 3 attempts + 1s + 2s backoff + Redis writes + buffer.
        // 150s ensures a legitimately in-flight webhook is never reclaimed and re-sent concurrently.
        const long claimIdleMs = 150_000;
        var startId = "0-0";

        while (!stoppingToken.IsCancellationRequested)
        {
            var result = await db.StreamAutoClaimAsync(
                StreamKey, ConsumerGroup, _consumerId,
                minIdleTimeInMs: claimIdleMs,
                startAtId: startId,
                count: 10);

            foreach (var entry in result.ClaimedEntries)
            {
                var payload = ExtractPayload(entry.Values);
                if (payload is null)
                {
                    _logger.LogError(
                        "Reclaimed stream entry {EntryId} is missing 'payload' field — writing to DLQ and ACKing",
                        entry.Id);
                    await WriteMalformedAndAckAsync(db, entry.Id, "(missing payload field)",
                        "payload field absent", source: "reclaimed");
                    continue;
                }

                NotificationJob job;
                try { job = NotificationJob.FromJson(payload); }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Malformed payload in reclaimed entry {EntryId} — writing to DLQ and ACKing", entry.Id);
                    await WriteMalformedAndAckAsync(db, entry.Id, payload, ex.Message, source: "reclaimed");
                    continue;
                }

                _logger.LogWarning("Reclaiming stale job {JobId} (idle >{IdleMs}ms)", job.JobId, claimIdleMs);
                await ProcessJobAsync(db, entry.Id, job, stoppingToken);
            }

            if (result.NextStartId == "0-0")
                break;

            startId = result.NextStartId;
        }
    }

    // Returns null when the 'payload' field is absent from the stream entry.
    private static string? ExtractPayload(NameValueEntry[] values)
    {
        var field = values.FirstOrDefault(v => v.Name == "payload");
        return field.Name == default ? null : field.Value.ToString();
    }

    // Writes a malformed entry to the DLQ and ACKs it. If the DLQ write fails, the entry is
    // left in the PEL so XAUTOCLAIM can reclaim it on the next pass.
    private async Task WriteMalformedAndAckAsync(IDatabase db, RedisValue entryId,
        string rawPayload, string parseError, string source)
    {
        try
        {
            await _dlq.WriteMalformedAsync(entryId.ToString(), rawPayload, parseError, source);
        }
        catch (Exception dlqEx)
        {
            _logger.LogWarning(dlqEx,
                "Failed to write malformed entry {EntryId} to DLQ — leaving in PEL for reclaim",
                entryId);
            return; // do not ACK: leave in PEL so XAUTOCLAIM retries
        }
        await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entryId);
    }

    private async Task ProcessJobAsync(IDatabase db, RedisValue entryId,
        NotificationJob job, CancellationToken stoppingToken)
    {
        // Guard: if this job already reached a terminal state (e.g., worker crashed after
        // delivery but before ACK, and the entry was reclaimed), skip re-sending.
        var currentStatus = await _statusStore.GetAsync(job.JobId);
        if (currentStatus?.State is DeliveryState.Delivered or DeliveryState.DeadLettered)
        {
            _logger.LogInformation(
                "Job {JobId} already in terminal state {State} — ACKing without re-sending",
                job.JobId, currentStatus.State);
            await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entryId);
            return;
        }

        DateTimeOffset? firstFailureTime = null;
        string? lastError = null;

        for (int attempt = 0; attempt < _retryPolicy.MaxAttempts; attempt++)
        {
            try
            {
                await _statusStore.UpdateStateAsync(
                    job.JobId, DeliveryState.Processing, attempt + 1);

                await _router.RouteAsync(job, stoppingToken);

                // Success
                await _statusStore.UpdateStateAsync(
                    job.JobId, DeliveryState.Delivered, attempt + 1);
                await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entryId);

                _logger.LogInformation("Job {JobId} delivered on attempt {Attempt}",
                    job.JobId, attempt + 1);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                firstFailureTime ??= DateTimeOffset.UtcNow;
                lastError = ex.Message;

                _logger.LogWarning(ex, "Job {JobId} attempt {Attempt} failed: {Error}",
                    job.JobId, attempt + 1, ex.Message);

                if (_retryPolicy.ShouldRetry(attempt + 1))
                {
                    var delay = _retryPolicy.GetDelay(attempt);
                    await Task.Delay(delay, stoppingToken);
                }
            }
        }

        // All retries exhausted — dead-letter
        await _dlq.WriteAsync(job, _retryPolicy.MaxAttempts, lastError!,
            firstFailureTime!.Value, DateTimeOffset.UtcNow);
        await _statusStore.UpdateStateAsync(
            job.JobId, DeliveryState.DeadLettered, _retryPolicy.MaxAttempts, lastError);
        await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entryId);

        _logger.LogError("Job {JobId} dead-lettered after {Attempts} attempts",
            job.JobId, _retryPolicy.MaxAttempts);
    }
}
