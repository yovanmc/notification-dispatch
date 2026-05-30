using Microsoft.AspNetCore.Mvc;
using NotificationDispatch.Core.Models;
using NotificationDispatch.Infrastructure;

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

    private static readonly HashSet<string> ValidChannels =
        new(StringComparer.OrdinalIgnoreCase) { "email", "sms", "webhook" };

    private const int MaxBodyLength = 10_000;
    private const int MaxRecipientLength = 1_000;

    private static string? ValidateRequest(NotificationRequest request)
    {
        if (!ValidChannels.Contains(request.Channel))
            return $"channel must be one of: email, sms, webhook. Got: '{request.Channel}'";

        if (string.IsNullOrWhiteSpace(request.Recipient))
            return "recipient is required";

        if (request.Recipient.Length > MaxRecipientLength)
            return $"recipient must not exceed {MaxRecipientLength} characters";

        if (string.IsNullOrWhiteSpace(request.Body))
            return "body is required";

        if (request.Body.Length > MaxBodyLength)
            return $"body must not exceed {MaxBodyLength} characters";

        return null; // valid
    }

    [HttpPost]
    public async Task<IActionResult> Send(
        [FromBody] NotificationRequest request,
        [FromHeader(Name = "Idempotency-Key")] string? idempotencyKey)
    {
        if (string.IsNullOrWhiteSpace(idempotencyKey))
            return BadRequest(new { error = "Idempotency-Key header is required" });

        if (idempotencyKey.Length > 256)
            return BadRequest(new { error = "Idempotency-Key must not exceed 256 characters" });

        var validationError = ValidateRequest(request);
        if (validationError is not null)
            return BadRequest(new { error = validationError });

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
        try
        {
            var status = await _statusStore.GetAsync(id);
            if (status is null)
                return NotFound(new { error = $"Job {id} not found" });

            return Ok(status);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve status for job {JobId}", id);
            return StatusCode(503, new { error = "Service temporarily unavailable" });
        }
    }

    [HttpGet("dlq")]
    public async Task<IActionResult> GetDlq([FromQuery] int count = 20)
    {
        if (count is < 1 or > 100)
            return BadRequest(new { error = "count must be between 1 and 100" });

        var entries = await _dlq.GetRecentAsync(count);
        return Ok(entries);
    }

    [HttpPost("dlq/{jobId}/replay")]
    public async Task<IActionResult> ReplayDlq(string jobId)
    {
        try
        {
            var dlqEntry = await _dlq.GetByJobIdAsync(jobId);
            if (dlqEntry is null)
                return NotFound(new { error = $"DLQ entry for job {jobId} not found" });

            if (!dlqEntry.TryGetValue("payload", out var payloadJson))
                return UnprocessableEntity(new { error = "DLQ entry is missing payload field" });

            var originalJob = NotificationJob.FromJson(payloadJson);
            var replayKey = $"replay-{jobId}-{DateTimeOffset.UtcNow.Ticks}";

            var (_, newJobId) = await _producer.EnqueueAsync(originalJob.Request, replayKey);

            _logger.LogInformation("DLQ job {OriginalJobId} replayed as {NewJobId}",
                jobId, newJobId);

            return AcceptedAtAction(nameof(GetStatus), new { id = newJobId },
                new { originalJobId = jobId, newJobId });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to replay DLQ job {JobId}", jobId);
            return StatusCode(503, new { error = "Service temporarily unavailable" });
        }
    }
}
