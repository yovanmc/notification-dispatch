using NotificationDispatch.Core;
using NotificationDispatch.Core.Models;
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
                var payload = entry.Values.First(v => v.Name == "payload").Value.ToString();

                NotificationJob job;
                try
                {
                    job = NotificationJob.FromJson(payload);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Malformed payload in stream entry {EntryId}, ACKing and skipping", entry.Id);
                    await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entry.Id);
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
        const long claimIdleMs = 30_000; // 30s — well above max retry duration (~12s)
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
                var payloadField = entry.Values.FirstOrDefault(v => v.Name == "payload");
                if (payloadField.Name == default)
                {
                    await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entry.Id);
                    continue;
                }

                NotificationJob job;
                try { job = NotificationJob.FromJson(payloadField.Value.ToString()); }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Malformed payload in reclaimed entry {EntryId}, ACKing and skipping", entry.Id);
                    await db.StreamAcknowledgeAsync(StreamKey, ConsumerGroup, entry.Id);
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

    private async Task ProcessJobAsync(IDatabase db, RedisValue entryId,
        NotificationJob job, CancellationToken stoppingToken)
    {
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
