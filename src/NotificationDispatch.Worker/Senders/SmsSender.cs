using Microsoft.Extensions.Logging;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;

namespace NotificationDispatch.Worker.Senders;

public class SmsSender : INotificationSender
{
    private readonly ILogger<SmsSender> _logger;

    public SmsSender(ILogger<SmsSender> logger)
    {
        _logger = logger;
    }

    public bool CanHandle(string channel) =>
        channel.Equals("sms", StringComparison.OrdinalIgnoreCase);

    public Task SendAsync(NotificationJob job, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "SMS sent to {Recipient} for job {JobId}",
            job.Request.Recipient, job.JobId);
        return Task.CompletedTask;
    }
}
