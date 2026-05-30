using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;

namespace NotificationDispatch.Worker.Senders;

public class WebhookSender : INotificationSender
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<WebhookSender> _logger;

    public WebhookSender(HttpClient httpClient, ILogger<WebhookSender> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
    }

    public bool CanHandle(string channel) =>
        channel.Equals("webhook", StringComparison.OrdinalIgnoreCase);

    public async Task SendAsync(NotificationJob job, CancellationToken cancellationToken = default)
    {
        // Partial validation: ensures the recipient is an absolute http/https URI.
        // This does NOT prevent SSRF — localhost, private IP ranges, link-local,
        // and cloud metadata endpoints (e.g. 169.254.169.254) are still reachable.
        // Full SSRF mitigation requires hostname resolution + IP range allowlist,
        // which is out of scope for this service.
        if (!Uri.TryCreate(job.Request.Recipient, UriKind.Absolute, out var uri) ||
            uri.Scheme is not ("http" or "https"))
        {
            throw new ArgumentException(
                $"Recipient must be an absolute http/https URL: {job.Request.Recipient}");
        }

        var payload = JsonSerializer.Serialize(new
        {
            jobId = job.JobId,
            channel = job.Request.Channel,
            recipient = job.Request.Recipient,
            body = job.Request.Body,
            metadata = job.Request.Metadata
        });

        var content = new StringContent(payload, Encoding.UTF8, "application/json");
        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, job.Request.Recipient)
        {
            Content = content
        };
        // Downstream idempotency hint — lets the webhook target deduplicate at-least-once redeliveries.
        httpRequest.Headers.TryAddWithoutValidation("X-Idempotency-Key", job.JobId);
        using var response = await _httpClient.SendAsync(httpRequest, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var rawBody = await response.Content.ReadAsStringAsync(cancellationToken);
            var body = rawBody.Length > 500 ? rawBody[..500] + "…" : rawBody;
            throw new HttpRequestException(
                $"Webhook to {job.Request.Recipient} returned {(int)response.StatusCode}: {body}",
                null, response.StatusCode);
        }

        _logger.LogInformation(
            "Webhook delivered to {Url} for job {JobId}, status {StatusCode}",
            job.Request.Recipient, job.JobId, (int)response.StatusCode);
    }
}
