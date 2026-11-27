using Microsoft.EntityFrameworkCore;
using Payments.Core.Data;
using Payments.Core.Domain;
using Payments.Core.Services;
using Xunit;

namespace Payments.Tests;

public class PaymentServiceTests
{
    private static PaymentsDbContext NewDb() =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options);

    private static readonly CreatePaymentRequest Request = new(49.90m, "EUR", "tok_visa_4242");

    [Fact]
    public async Task First_request_creates_payment_and_outbox_message()
    {
        await using var db = NewDb();
        var service = new PaymentService(db, TimeProvider.System);

        var result = await service.CreateAsync("merchant-1", "key-1", Request);

        Assert.Equal(CreatePaymentOutcome.Created, result.Outcome);
        Assert.Equal(1, await db.Payments.CountAsync());
        Assert.Equal(1, await db.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Same_key_and_same_body_returns_the_original_payment()
    {
        await using var db = NewDb();
        var service = new PaymentService(db, TimeProvider.System);

        var first = await service.CreateAsync("merchant-1", "key-1", Request);
        var retry = await service.CreateAsync("merchant-1", "key-1", Request);

        Assert.Equal(CreatePaymentOutcome.Replayed, retry.Outcome);
        Assert.Equal(first.Payment!.Id, retry.Payment!.Id);
        Assert.Equal(1, await db.Payments.CountAsync());
        Assert.Equal(1, await db.OutboxMessages.CountAsync());
    }

    [Fact]
    public async Task Same_key_with_different_body_is_rejected()
    {
        await using var db = NewDb();
        var service = new PaymentService(db, TimeProvider.System);

        await service.CreateAsync("merchant-1", "key-1", Request);
        var result = await service.CreateAsync("merchant-1", "key-1", Request with { Amount = 99m });

        Assert.Equal(CreatePaymentOutcome.KeyReusedWithDifferentRequest, result.Outcome);
        Assert.Equal(1, await db.Payments.CountAsync());
    }

    [Fact]
    public async Task Same_key_for_two_merchants_creates_two_payments()
    {
        await using var db = NewDb();
        var service = new PaymentService(db, TimeProvider.System);

        await service.CreateAsync("merchant-1", "key-1", Request);
        var other = await service.CreateAsync("merchant-2", "key-1", Request);

        Assert.Equal(CreatePaymentOutcome.Created, other.Outcome);
        Assert.Equal(2, await db.Payments.CountAsync());
    }

    [Fact]
    public void Request_hash_ignores_currency_case_and_trailing_zeros()
    {
        var a = PaymentService.Hash(new CreatePaymentRequest(49.9m, "eur", "tok_visa_4242"));
        var b = PaymentService.Hash(new CreatePaymentRequest(49.90m, "EUR", "tok_visa_4242"));

        Assert.Equal(a, b);
    }

    [Fact]
    public async Task Currency_with_surrounding_spaces_is_accepted_and_normalized()
    {
        await using var db = NewDb();
        var service = new PaymentService(db, TimeProvider.System);

        var result = await service.CreateAsync("merchant-1", "key-1", Request with { Currency = " eur " });

        Assert.Equal(CreatePaymentOutcome.Created, result.Outcome);
        Assert.Equal("EUR", result.Payment!.Currency);
    }

    [Fact]
    public void Amount_with_more_than_two_decimals_is_rejected()
    {
        Assert.Throws<ArgumentException>(() =>
            Payment.Create("m", "k", "h", 49.904m, "EUR", "tok", DateTimeOffset.UtcNow));
    }

    [Fact]
    public void A_final_status_is_never_overwritten()
    {
        var payment = Payment.Create("m", "k", "h", 10m, "EUR", "tok", DateTimeOffset.UtcNow);

        payment.Authorize(DateTimeOffset.UtcNow);
        payment.Decline(DateTimeOffset.UtcNow);

        Assert.Equal(PaymentStatus.Authorized, payment.Status);
    }
}
