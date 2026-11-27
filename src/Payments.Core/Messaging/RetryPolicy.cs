namespace Payments.Core.Messaging;

/// <summary>Retry rules shared by the outbox publisher and the consumer. Pure functions, easy to test.</summary>
public static class RetryPolicy
{
    public static readonly TimeSpan MaxBackoff = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Exponential backoff for the n-th failed attempt (1-based): 2 s, 4 s, 8 s, ... capped at 5 minutes.
    /// </summary>
    public static TimeSpan Backoff(int failedAttempts)
    {
        if (failedAttempts < 1) return TimeSpan.Zero;
        var exponent = Math.Min(failedAttempts, 20); // avoid overflow; the cap applies long before
        var seconds = Math.Pow(2, exponent);
        var delay = TimeSpan.FromSeconds(seconds);
        return delay > MaxBackoff ? MaxBackoff : delay;
    }

    /// <summary>
    /// A message that has already been retried <paramref name="retryCount"/> times goes to the
    /// dead-letter queue once it reaches <paramref name="maxRetries"/>.
    /// </summary>
    public static bool ShouldDeadLetter(int retryCount, int maxRetries) => retryCount >= maxRetries;
}
