using NotificationDispatch.Core.Models;

namespace NotificationDispatch.Core.Interfaces;

public interface INotificationSender
{
    bool CanHandle(string channel);
    Task SendAsync(NotificationJob job, CancellationToken cancellationToken = default);
}
