namespace NotificationDispatch.Core.Models;

public enum DeliveryState
{
    Queued,
    Processing,
    Delivered,
    DeadLettered
}
