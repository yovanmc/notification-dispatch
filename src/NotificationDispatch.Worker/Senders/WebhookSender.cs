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
        var payload = JsonSerializer.Serialize(new
        {
            jobId = job.JobId,
            channel = job.Request.Channel,
            recipient = job.Request.Recipient,
            body = job.Request.Body,
            metadata = job.Request.Metadata
        });

        var content = new StringContent(payload, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(job.Request.Recipient, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException(
                $"Webhook to {job.Request.Recipient} returned {(int)response.StatusCode}: {body}",
                null, response.StatusCode);
        }

        _logger.LogInformation(
            "Webhook delivered to {Url} for job {JobId}, status {StatusCode}",
            job.Request.Recipient, job.JobId, (int)response.StatusCode);
    }
}
