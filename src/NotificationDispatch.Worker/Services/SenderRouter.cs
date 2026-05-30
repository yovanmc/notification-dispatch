using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Core.Models;

namespace NotificationDispatch.Worker.Services;

public class SenderRouter
{
    private readonly IEnumerable<INotificationSender> _senders;

    public SenderRouter(IEnumerable<INotificationSender> senders)
    {
        _senders = senders;
    }

    public INotificationSender GetSender(string channel)
    {
        return _senders.FirstOrDefault(s => s.CanHandle(channel))
            ?? throw new InvalidOperationException($"No sender registered for channel '{channel}'");
    }

    public async Task RouteAsync(NotificationJob job, CancellationToken cancellationToken = default)
    {
        var sender = GetSender(job.Request.Channel);
        await sender.SendAsync(job, cancellationToken);
    }
}
