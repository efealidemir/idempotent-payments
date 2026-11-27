using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Payments.Core.Data;
using Payments.Core.Domain;
using Payments.Core.Services;
using Xunit;

namespace Payments.Tests;

/// <summary>
/// Two identical requests race: both see "no payment yet", both try to insert.
/// The unique index (MerchantId, IdempotencyKey) must let exactly one win, and the loser
/// must return the winner's payment instead of failing or creating a duplicate.
/// SQLite is used (not the EF in-memory provider) because it enforces unique indexes.
/// </summary>
public sealed class ConcurrentRequestTests : IDisposable
{
    private readonly SqliteConnection _connection;

    public ConcurrentRequestTests()
    {
        _connection = new SqliteConnection("DataSource=:memory:");
        _connection.Open();
        using var db = CreateDb();
        db.Database.EnsureCreated();
    }

    public void Dispose() => _connection.Dispose();

    private PaymentsDbContext CreateDb(params IInterceptor[] interceptors) =>
        new(new DbContextOptionsBuilder<PaymentsDbContext>()
            .UseSqlite(_connection)
            .AddInterceptors(interceptors)
            .Options);

    private static readonly CreatePaymentRequest Request = new(49.90m, "EUR", "tok_visa_4242");

    [Fact]
    public async Task Losing_request_of_a_race_returns_the_winning_payment()
    {
        // The interceptor lets a "parallel" request commit first, right before our own insert.
        var competitor = new CommitCompetingRequestFirst(CreateDb, "merchant-1", "key-1", Request);
        await using var db = CreateDb(competitor);
        var service = new PaymentService(db, TimeProvider.System);

        var result = await service.CreateAsync("merchant-1", "key-1", Request);

        await using var check = CreateDb();
        Assert.Equal(CreatePaymentOutcome.Replayed, result.Outcome);
        Assert.Equal(competitor.WinnerId, result.Payment!.Id);
        Assert.Equal(1, await check.Payments.CountAsync());
    }

    [Fact]
    public async Task Race_with_a_different_body_under_the_same_key_is_rejected()
    {
        var competitor = new CommitCompetingRequestFirst(CreateDb, "merchant-1", "key-1",
            Request with { Amount = 10m });
        await using var db = CreateDb(competitor);
        var service = new PaymentService(db, TimeProvider.System);

        var result = await service.CreateAsync("merchant-1", "key-1", Request);

        await using var check = CreateDb();
        Assert.Equal(CreatePaymentOutcome.KeyReusedWithDifferentRequest, result.Outcome);
        Assert.Equal(1, await check.Payments.CountAsync());
        Assert.Equal(1, await check.OutboxMessages.CountAsync()); // only the winner's event survives
    }

    /// <summary>Simulates the other request of the race: commits its payment just before ours is saved.</summary>
    private sealed class CommitCompetingRequestFirst : SaveChangesInterceptor
    {
        private readonly Func<IInterceptor[], PaymentsDbContext> _createDb;
        private readonly string _merchantId;
        private readonly string _key;
        private readonly CreatePaymentRequest _request;
        private bool _done;

        public Guid WinnerId { get; private set; }

        public CommitCompetingRequestFirst(Func<IInterceptor[], PaymentsDbContext> createDb,
            string merchantId, string key, CreatePaymentRequest request)
        {
            _createDb = createDb;
            _merchantId = merchantId;
            _key = key;
            _request = request;
        }

        public override async ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            if (!_done)
            {
                _done = true;
                await using var other = _createDb(Array.Empty<IInterceptor>());
                var winner = Payment.Create(_merchantId, _key, PaymentService.Hash(_request),
                    _request.Amount, _request.Currency, _request.CardToken, DateTimeOffset.UtcNow);
                other.Payments.Add(winner);
                other.OutboxMessages.Add(new OutboxMessage
                {
                    Type = nameof(PaymentCreated),
                    Payload = "{}",
                    OccurredAt = DateTimeOffset.UtcNow
                });
                await other.SaveChangesAsync(cancellationToken);
                WinnerId = winner.Id;
            }

            return result;
        }
    }
}
