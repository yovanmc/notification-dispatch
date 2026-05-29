using Microsoft.AspNetCore.Mvc;
using NotificationDispatch.Api.Services;
using NotificationDispatch.Core.Models;
using NotificationDispatch.Worker.Services;

namespace NotificationDispatch.Api.Controllers;

[ApiController]
[Route("[controller]")]
public class NotificationsController : ControllerBase
{
    private readonly RedisStreamProducer _producer;
    private readonly RedisStatusStore _statusStore;
    private readonly DeadLetterStore _dlq;
    private readonly ILogger<NotificationsController> _logger;

    public NotificationsController(
        RedisStreamProducer producer,
        RedisStatusStore statusStore,
        DeadLetterStore dlq,
        ILogger<NotificationsController> logger)
    {
        _producer = producer;
        _statusStore = statusStore;
        _dlq = dlq;
        _logger = logger;
    }

    [HttpPost]
    public async Task<IActionResult> Send(
        [FromBody] NotificationRequest request,
        [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey))
            return BadRequest(new { error = "Idempotency-Key header is required" });

        try
        {
            var (created, jobId) = await _producer.EnqueueAsync(request, idempotencyKey);

            if (!created)
            {
                var existingStatus = await _statusStore.GetAsync(jobId);
                _logger.LogInformation("Duplicate request with idempotency key, returning existing job {JobId}", jobId);
                return Ok(new { jobId, status = existingStatus });
            }

            _logger.LogInformation("Job {JobId} enqueued for channel {Channel}", jobId, request.Channel);
            return AcceptedAtAction(nameof(GetStatus), new { id = jobId }, new { jobId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to enqueue notification");
            return StatusCode(503, new { error = "Service temporarily unavailable" });
        }
    }

    [HttpGet("{id}")]
    public async Task<IActionResult> GetStatus(string id)
    {
        var status = await _statusStore.GetAsync(id);
        if (status is null)
            return NotFound(new { error = $"Job {id} not found" });

        return Ok(status);
    }

    [HttpGet("dlq")]
    public async Task<IActionResult> GetDlq([FromQuery] int count = 20)
    {
        var entries = await _dlq.GetRecentAsync(count);
        return Ok(entries);
    }

    [HttpPost("dlq/{jobId}/replay")]
    public async Task<IActionResult> ReplayDlq(string jobId)
    {
        var dlqEntry = await _dlq.GetByJobIdAsync(jobId);
        if (dlqEntry is null)
            return NotFound(new { error = $"DLQ entry for job {jobId} not found" });

        var originalJob = NotificationJob.FromJson(dlqEntry["payload"]);
        var replayKey = $"replay-{jobId}-{DateTimeOffset.UtcNow.Ticks}";

        var (_, newJobId) = await _producer.EnqueueAsync(originalJob.Request, replayKey);

        _logger.LogInformation("DLQ job {OriginalJobId} replayed as {NewJobId}",
            jobId, newJobId);

        return AcceptedAtAction(nameof(GetStatus), new { id = newJobId },
            new { originalJobId = jobId, newJobId });
    }
}
