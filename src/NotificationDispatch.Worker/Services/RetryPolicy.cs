namespace NotificationDispatch.Worker.Services;

public class RetryPolicy
{
    public int MaxAttempts { get; } = 3;

    // Two delays for three attempts: fired between attempt 1→2 and attempt 2→3.
    // No delay after the final attempt.
    private static readonly TimeSpan[] Delays = [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
    ];

    public bool ShouldRetry(int attemptNumber) => attemptNumber < MaxAttempts;

    public TimeSpan GetDelay(int attemptNumber)
    {
        var index = Math.Min(attemptNumber, Delays.Length - 1);
        return Delays[index];
    }
}
