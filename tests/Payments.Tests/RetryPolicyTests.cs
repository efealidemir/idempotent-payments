using Payments.Core.Messaging;
using Xunit;

namespace Payments.Tests;

public class RetryPolicyTests
{
    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    [InlineData(3, 8)]
    [InlineData(6, 64)]
    public void Backoff_doubles_with_each_failed_attempt(int attempt, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), RetryPolicy.Backoff(attempt));
    }

    [Theory]
    [InlineData(9)]
    [InlineData(50)]
    [InlineData(int.MaxValue)]
    public void Backoff_is_capped_at_five_minutes(int attempt)
    {
        Assert.Equal(RetryPolicy.MaxBackoff, RetryPolicy.Backoff(attempt));
    }

    [Fact]
    public void No_failure_means_no_delay()
    {
        Assert.Equal(TimeSpan.Zero, RetryPolicy.Backoff(0));
    }

    [Theory]
    [InlineData(0, false)]
    [InlineData(2, false)]
    [InlineData(3, true)]
    [InlineData(4, true)]
    public void Message_is_dead_lettered_after_max_retries(int retryCount, bool expected)
    {
        Assert.Equal(expected, RetryPolicy.ShouldDeadLetter(retryCount, maxRetries: 3));
    }
}
