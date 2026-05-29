namespace NotificationDispatch.Worker.Services;

public class RetryPolicy
{
    public int MaxAttempts { get; } = 3;

    private static readonly TimeSpan[] Delays = [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(4)
    ];

    public bool ShouldRetry(int attemptNumber) => attemptNumber < MaxAttempts;

    public TimeSpan GetDelay(int attemptNumber)
    {
        var index = Math.Min(attemptNumber, Delays.Length - 1);
        return Delays[index];
    }
}
