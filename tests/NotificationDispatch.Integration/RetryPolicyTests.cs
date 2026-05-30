using NotificationDispatch.Worker.Services;

namespace NotificationDispatch.Integration;

// Pure unit tests — no Redis, no Testcontainers, no fixture needed.
public class RetryPolicyTests
{
    private readonly RetryPolicy _retry = new();

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, true)]
    [InlineData(2, true)]
    [InlineData(3, false)]
    [InlineData(10, false)]
    public void ShouldRetry_RespectsMaxAttempts(int attempt, bool expected)
    {
        Assert.Equal(expected, _retry.ShouldRetry(attempt));
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 2)] // clamped to last delay (2s) — 4s is never used with MaxAttempts=3
    [InlineData(3, 2)] // clamped
    [InlineData(10, 2)] // clamped
    public void GetDelay_ReturnsCorrectBackoff(int attempt, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), _retry.GetDelay(attempt));
    }
}
