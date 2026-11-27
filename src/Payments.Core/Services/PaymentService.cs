using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Payments.Core.Data;
using Payments.Core.Domain;

namespace Payments.Core.Services;

public record CreatePaymentRequest(decimal Amount, string Currency, string CardToken);

public enum CreatePaymentOutcome
{
    Created,
    Replayed,
    KeyReusedWithDifferentRequest
}

public record CreatePaymentResult(CreatePaymentOutcome Outcome, Payment? Payment);

public class PaymentService
{
    private readonly PaymentsDbContext _db;
    private readonly TimeProvider _clock;

    public PaymentService(PaymentsDbContext db, TimeProvider clock)
    {
        _db = db;
        _clock = clock;
    }

    public async Task<CreatePaymentResult> CreateAsync(
        string merchantId,
        string idempotencyKey,
        CreatePaymentRequest request,
        CancellationToken ct = default)
    {
        var requestHash = Hash(request);

        var existing = await FindAsync(merchantId, idempotencyKey, ct);
        if (existing is not null)
            return Compare(existing, requestHash);

        var now = _clock.GetUtcNow();
        var payment = Payment.Create(merchantId, idempotencyKey, requestHash,
            request.Amount, request.Currency, request.CardToken, now);

        _db.Payments.Add(payment);
        _db.OutboxMessages.Add(new OutboxMessage
        {
            Type = nameof(PaymentCreated),
            Payload = JsonSerializer.Serialize(
                new PaymentCreated(payment.Id, payment.MerchantId, payment.Amount, payment.Currency)),
            OccurredAt = now
        });

        try
        {
            // Payment + outbox row are committed atomically by a single SaveChanges.
            await _db.SaveChangesAsync(ct);
            return new CreatePaymentResult(CreatePaymentOutcome.Created, payment);
        }
        catch (DbUpdateException)
        {
            // Another request with the same key won the race (unique index violation).
            // Forget our tracked entities and return what the winner stored.
            _db.ChangeTracker.Clear();
            var winner = await FindAsync(merchantId, idempotencyKey, ct);
            if (winner is null) throw;
            return Compare(winner, requestHash);
        }
    }

    public Task<Payment?> GetAsync(string merchantId, Guid id, CancellationToken ct = default) =>
        _db.Payments.AsNoTracking()
            .FirstOrDefaultAsync(p => p.Id == id && p.MerchantId == merchantId, ct);

    private Task<Payment?> FindAsync(string merchantId, string key, CancellationToken ct) =>
        _db.Payments.AsNoTracking()
            .FirstOrDefaultAsync(p => p.MerchantId == merchantId && p.IdempotencyKey == key, ct);

    private static CreatePaymentResult Compare(Payment existing, string requestHash) =>
        existing.RequestHash == requestHash
            ? new CreatePaymentResult(CreatePaymentOutcome.Replayed, existing)
            : new CreatePaymentResult(CreatePaymentOutcome.KeyReusedWithDifferentRequest, null);

    public static string Hash(CreatePaymentRequest request)
    {
        // Normalize so that "eur" and "EUR", or 49.9 and 49.90, hash the same.
        var normalized = string.Create(CultureInfo.InvariantCulture,
            $"{request.Amount:0.00}|{request.Currency.Trim().ToUpperInvariant()}|{request.CardToken.Trim()}");
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(normalized));
        return Convert.ToHexString(bytes);
    }
}
