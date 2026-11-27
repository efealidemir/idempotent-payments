using Microsoft.EntityFrameworkCore;
using Payments.Core.Domain;

namespace Payments.Core.Data;

public class PaymentsDbContext : DbContext
{
    public PaymentsDbContext(DbContextOptions<PaymentsDbContext> options) : base(options) { }

    public DbSet<Payment> Payments => Set<Payment>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<ProcessedMessage> ProcessedMessages => Set<ProcessedMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Payment>(b =>
        {
            b.HasKey(p => p.Id);
            b.Property(p => p.MerchantId).HasMaxLength(64).IsRequired();
            b.Property(p => p.IdempotencyKey).HasMaxLength(128).IsRequired();
            b.Property(p => p.RequestHash).HasMaxLength(64).IsRequired();
            b.Property(p => p.Amount).HasPrecision(18, 2);
            b.Property(p => p.Currency).HasMaxLength(3).IsRequired();
            b.Property(p => p.CardToken).HasMaxLength(128).IsRequired();
            b.Property(p => p.Status).HasConversion<string>().HasMaxLength(16);

            // The core of idempotency: one payment per merchant + key, enforced by the database.
            b.HasIndex(p => new { p.MerchantId, p.IdempotencyKey }).IsUnique();
        });

        modelBuilder.Entity<OutboxMessage>(b =>
        {
            b.HasKey(m => m.Id);
            b.Property(m => m.Type).HasMaxLength(128).IsRequired();
            b.HasIndex(m => m.PublishedAt);
        });

        modelBuilder.Entity<ProcessedMessage>(b =>
        {
            b.HasKey(m => new { m.MessageId, m.Consumer });
            b.Property(m => m.Consumer).HasMaxLength(64);
        });
    }
}
