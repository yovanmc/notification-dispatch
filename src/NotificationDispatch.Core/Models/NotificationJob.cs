using System.Text.Json;

namespace NotificationDispatch.Core.Models;

public record NotificationJob
{
    public required string JobId { get; init; }
    public required NotificationRequest Request { get; init; }
    public required DateTimeOffset EnqueuedAt { get; init; }

    public string ToJson() => JsonSerializer.Serialize(this);

    public static NotificationJob FromJson(string json) =>
        JsonSerializer.Deserialize<NotificationJob>(json)
        ?? throw new InvalidOperationException("Failed to deserialize NotificationJob");
}
