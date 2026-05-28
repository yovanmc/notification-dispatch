using NotificationDispatch.Core.Models;

namespace NotificationDispatch.Core.Interfaces;

public interface IStatusStore
{
    Task SetAsync(NotificationStatus status);
    Task<NotificationStatus?> GetAsync(string jobId);
    Task UpdateStateAsync(string jobId, DeliveryState state, int attempts, string? lastError = null);
}
