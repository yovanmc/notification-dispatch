using Microsoft.Extensions.Logging;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;

namespace NotificationDispatch.Worker.Senders;

public class EmailSender : INotificationSender
{
    private readonly ILogger<EmailSender> _logger;

    public EmailSender(ILogger<EmailSender> logger)
    {
        _logger = logger;
    }

    public bool CanHandle(string channel) =>
        channel.Equals("email", StringComparison.OrdinalIgnoreCase);

    public Task SendAsync(NotificationJob job, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "[FAKE] Email sent to {Recipient} with subject {Subject} for job {JobId}",
            job.Request.Recipient, job.Request.Subject, job.JobId);
        return Task.CompletedTask;
    }
}
