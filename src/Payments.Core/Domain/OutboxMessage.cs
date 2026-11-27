namespace Payments.Core.Domain;

/// <summary>An integration event saved in the same transaction as the business change.</summary>
public class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Type { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset? PublishedAt { get; set; }
    public int Attempts { get; set; }
    public string? LastError { get; set; }

    /// <summary>Earliest time of the next publish attempt after a failure (exponential backoff).</summary>
    public DateTimeOffset? NextAttemptAt { get; set; }
}

/// <summary>Message ids already handled by a consumer (idempotent consumer pattern).</summary>
public class ProcessedMessage
{
    public Guid MessageId { get; set; }
    public string Consumer { get; set; } = default!;
    public DateTimeOffset ProcessedAt { get; set; }
}

public record PaymentCreated(Guid PaymentId, string MerchantId, decimal Amount, string Currency);
