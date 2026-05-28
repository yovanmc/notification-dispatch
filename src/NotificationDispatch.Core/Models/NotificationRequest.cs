namespace NotificationDispatch.Core.Models;

public record NotificationRequest
{
    public required string Channel { get; init; }
    public required string Recipient { get; init; }
    public string? Subject { get; init; }
    public required string Body { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }
}
