namespace NotificationDispatch.Core.Models;

public enum DeliveryState
{
    Queued,
    Processing,
    Delivered,
    DeadLettered
}

public record NotificationStatus
{
    public required string JobId { get; init; }
    public required string Channel { get; init; }
    public required DeliveryState State { get; init; }
    public int Attempts { get; init; }
    public string? LastError { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
}
