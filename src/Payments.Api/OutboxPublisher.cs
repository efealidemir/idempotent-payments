using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Payments.Core.Data;
using Payments.Core.Messaging;
using RabbitMQ.Client;

namespace Payments.Api;

/// <summary>
/// Polls unpublished outbox rows and publishes them to RabbitMQ with publisher confirms.
/// A row is marked published only after the broker confirms it: at-least-once delivery.
/// </summary>
public class OutboxPublisher : BackgroundService
{
    private const int BatchSize = 50;
    private const int MaxAttempts = 10;
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    private readonly IServiceScopeFactory _scopes;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<OutboxPublisher> _logger;
    private readonly TimeProvider _clock;

    public OutboxPublisher(IServiceScopeFactory scopes, IOptions<RabbitMqOptions> options,
        ILogger<OutboxPublisher> logger, TimeProvider clock)
    {
        _scopes = scopes;
        _options = options.Value;
        _logger = logger;
        _clock = clock;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        IConnection? connection = null;
        IModel? channel = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (channel is null || channel.IsClosed)
                {
                    // WaitForConfirmsOrDie closes the channel on nack/timeout: release the old
                    // channel and connection before opening new ones, otherwise they leak.
                    channel?.Dispose();
                    connection?.Dispose();
                    connection = RabbitMq.Connect(_options);
                    channel = connection.CreateModel();
                    RabbitMq.DeclareTopology(channel, _options.QueueName);
                    channel.ConfirmSelect();
                }

                await PublishBatchAsync(channel, stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogWarning(ex, "Outbox publishing failed; retrying");
                channel?.Dispose();
                connection?.Dispose();
                channel = null;
                connection = null;
            }

            await Task.Delay(PollInterval, stoppingToken);
        }

        channel?.Dispose();
        connection?.Dispose();
    }

    private async Task PublishBatchAsync(IModel channel, CancellationToken ct)
    {
        using var scope = _scopes.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();

        var now = _clock.GetUtcNow();
        var messages = await db.OutboxMessages
            .Where(m => m.PublishedAt == null && m.Attempts < MaxAttempts
                        && (m.NextAttemptAt == null || m.NextAttemptAt <= now))
            .OrderBy(m => m.OccurredAt)
            .Take(BatchSize)
            .ToListAsync(ct);

        foreach (var message in messages)
        {
            try
            {
                var props = channel.CreateBasicProperties();
                props.Persistent = true;
                props.MessageId = message.Id.ToString(); // consumers deduplicate on this id
                props.Type = message.Type;

                channel.BasicPublish("", _options.QueueName, props, Encoding.UTF8.GetBytes(message.Payload));
                channel.WaitForConfirmsOrDie(TimeSpan.FromSeconds(5));

                message.PublishedAt = _clock.GetUtcNow();
            }
            catch (Exception ex)
            {
                message.Attempts++;
                message.LastError = ex.Message;
                message.NextAttemptAt = _clock.GetUtcNow() + RetryPolicy.Backoff(message.Attempts);
                _logger.LogWarning(ex,
                    "Could not publish outbox message {MessageId} (attempt {Attempt}); next try at {NextAttemptAt}",
                    message.Id, message.Attempts, message.NextAttemptAt);
                break; // keep ordering: stop this batch, retry after the backoff delay
            }
        }

        await db.SaveChangesAsync(ct);
    }
}
