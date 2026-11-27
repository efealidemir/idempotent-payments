namespace Payments.Core.Domain;

public enum PaymentStatus
{
    Pending = 0,
    Authorized = 1,
    Declined = 2
}

public class Payment
{
    public Guid Id { get; private set; }
    public string MerchantId { get; private set; } = default!;
    public string IdempotencyKey { get; private set; } = default!;
    public string RequestHash { get; private set; } = default!;
    public decimal Amount { get; private set; }
    public string Currency { get; private set; } = default!;
    public string CardToken { get; private set; } = default!;
    public PaymentStatus Status { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }
    public DateTimeOffset? UpdatedAt { get; private set; }

    private Payment() { } // EF Core

    public static Payment Create(
        string merchantId,
        string idempotencyKey,
        string requestHash,
        decimal amount,
        string currency,
        string cardToken,
        DateTimeOffset now)
    {
        if (amount <= 0) throw new ArgumentOutOfRangeException(nameof(amount), "Amount must be positive.");
        if (decimal.Round(amount, 2) != amount)
            throw new ArgumentException("Amount cannot have more than 2 decimal places.", nameof(amount));
        currency = currency?.Trim() ?? string.Empty;
        if (currency.Length != 3)
            throw new ArgumentException("Currency must be a 3-letter ISO code.", nameof(currency));

        return new Payment
        {
            Id = Guid.NewGuid(),
            MerchantId = merchantId,
            IdempotencyKey = idempotencyKey,
            RequestHash = requestHash,
            Amount = amount,
            Currency = currency.ToUpperInvariant(),
            CardToken = cardToken.Trim(),
            Status = PaymentStatus.Pending,
            CreatedAt = now
        };
    }

    public void Authorize(DateTimeOffset now) => SetFinalStatus(PaymentStatus.Authorized, now);

    public void Decline(DateTimeOffset now) => SetFinalStatus(PaymentStatus.Declined, now);

    private void SetFinalStatus(PaymentStatus status, DateTimeOffset now)
    {
        if (Status != PaymentStatus.Pending)
            return; // already final: applying the same outcome twice must be a no-op

        Status = status;
        UpdatedAt = now;
    }
}
