using Microsoft.EntityFrameworkCore;
using Payments.Core.Data;
using Payments.Core.Domain;

namespace Payments.Worker;

public interface IBankAuthorizer
{
    Task<bool> AuthorizeAsync(PaymentCreated payment, CancellationToken ct);
}

/// <summary>Stands in for a real acquirer/bank call. Declines amounts above 10,000.</summary>
public class FakeBankAuthorizer : IBankAuthorizer
{
    public async Task<bool> AuthorizeAsync(PaymentCreated payment, CancellationToken ct)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(100), ct); // simulated network latency
        return payment.Amount <= 10_000m;
    }
}

/// <summary>
/// Idempotent consumer: a message id is handled at most once per consumer, even if RabbitMQ
/// delivers it several times. The status change and the "processed" marker share one transaction.
/// </summary>
public class PaymentAuthorizationHandler
{
    public const string ConsumerName = "payment-authorization";

    private readonly PaymentsDbContext _db;
    private readonly IBankAuthorizer _bank;
    private readonly TimeProvider _clock;
    private readonly ILogger<PaymentAuthorizationHandler> _logger;

    public PaymentAuthorizationHandler(PaymentsDbContext db, IBankAuthorizer bank,
        TimeProvider clock, ILogger<PaymentAuthorizationHandler> logger)
    {
        _db = db;
        _bank = bank;
        _clock = clock;
        _logger = logger;
    }

    public async Task HandleAsync(Guid messageId, PaymentCreated message, CancellationToken ct)
    {
        var alreadyProcessed = await _db.ProcessedMessages
            .AnyAsync(m => m.MessageId == messageId && m.Consumer == ConsumerName, ct);
        if (alreadyProcessed)
        {
            _logger.LogInformation("Duplicate message {MessageId} ignored", messageId);
            return;
        }

        var payment = await _db.Payments.FirstOrDefaultAsync(p => p.Id == message.PaymentId, ct);
        if (payment is null)
        {
            _logger.LogWarning("Payment {PaymentId} not found", message.PaymentId);
            return;
        }

        // Note: the bank call itself should also carry an idempotency key (the payment id)
        // so that a crash between the call and the commit does not charge twice.
        var authorized = await _bank.AuthorizeAsync(message, ct);
        var now = _clock.GetUtcNow();
        if (authorized) payment.Authorize(now); else payment.Decline(now);

        _db.ProcessedMessages.Add(new ProcessedMessage
        {
            MessageId = messageId,
            Consumer = ConsumerName,
            ProcessedAt = now
        });

        try
        {
            await _db.SaveChangesAsync(ct);
        }
        catch (DbUpdateException)
        {
            // A parallel delivery of the same message committed first (primary key on MessageId).
            _logger.LogInformation("Message {MessageId} was processed concurrently; ignoring", messageId);
        }
    }
}
