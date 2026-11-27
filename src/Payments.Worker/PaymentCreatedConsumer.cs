using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using Payments.Core.Domain;
using Payments.Core.Messaging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Payments.Worker;

public class PaymentCreatedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _scopes;
    private readonly RabbitMqOptions _options;
    private readonly ILogger<PaymentCreatedConsumer> _logger;
    private IConnection? _connection;
    private IModel? _channel;

    public PaymentCreatedConsumer(IServiceScopeFactory scopes, IOptions<RabbitMqOptions> options,
        ILogger<PaymentCreatedConsumer> logger)
    {
        _scopes = scopes;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // RabbitMQ may still be starting in docker compose: retry the connection.
        while (_channel is null && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                _connection = RabbitMq.Connect(_options);
                _channel = _connection.CreateModel();
                RabbitMq.DeclareTopology(_channel, _options.QueueName);
                _channel.BasicQos(0, prefetchCount: 10, global: false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "RabbitMQ not reachable yet; retrying in 5 s");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        if (_channel is null) return;

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += async (_, delivery) => await OnMessageAsync(delivery, stoppingToken);
        _channel.BasicConsume(_options.QueueName, autoAck: false, consumer: consumer);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }

    private async Task OnMessageAsync(BasicDeliverEventArgs delivery, CancellationToken ct)
    {
        // 1. Poison messages (unreadable) can never succeed: dead-letter them immediately.
        if (!Guid.TryParse(delivery.BasicProperties.MessageId, out var messageId)
            || !TryDeserialize(delivery, out var message))
        {
            _logger.LogError("Unreadable message (tag {DeliveryTag}); sending it to the dead-letter queue",
                delivery.DeliveryTag);
            DeadLetter(delivery);
            return;
        }

        try
        {
            using var scope = _scopes.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<PaymentAuthorizationHandler>();
            await handler.HandleAsync(messageId, message!, ct);

            _channel!.BasicAck(delivery.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            // 2. Transient failures (database down, bank timeout): retry a few times, then dead-letter.
            var retryCount = GetRetryCount(delivery.BasicProperties);
            if (RetryPolicy.ShouldDeadLetter(retryCount, MaxRetries))
            {
                _logger.LogError(ex, "Message {MessageId} failed after {RetryCount} retries; dead-lettering",
                    messageId, retryCount);
                DeadLetter(delivery);
                return;
            }

            _logger.LogWarning(ex, "Message {MessageId} failed (retry {Retry} of {MaxRetries}); requeueing",
                messageId, retryCount + 1, MaxRetries);
            Republish(delivery, retryCount + 1);
            // Ack only after the copy is published: at worst the message is processed twice,
            // which the idempotent handler absorbs; it is never lost.
            _channel!.BasicAck(delivery.DeliveryTag, multiple: false);
        }
    }

    private const int MaxRetries = 3;
    private const string RetryCountHeader = "x-retry-count";

    private static bool TryDeserialize(BasicDeliverEventArgs delivery, out PaymentCreated? message)
    {
        try
        {
            message = JsonSerializer.Deserialize<PaymentCreated>(Encoding.UTF8.GetString(delivery.Body.Span));
            return message is not null;
        }
        catch (JsonException)
        {
            message = null;
            return false;
        }
    }

    private static int GetRetryCount(IBasicProperties props)
    {
        if (props.Headers is not null
            && props.Headers.TryGetValue(RetryCountHeader, out var value)
            && value is not null)
        {
            return value switch
            {
                int i => i,
                long l => (int)l,
                byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) => parsed,
                _ => 0
            };
        }

        return 0;
    }

    private void Republish(BasicDeliverEventArgs delivery, int retryCount)
    {
        var props = _channel!.CreateBasicProperties();
        props.Persistent = true;
        props.MessageId = delivery.BasicProperties.MessageId; // same id: deduplication still works
        props.Type = delivery.BasicProperties.Type;
        props.Headers = new Dictionary<string, object> { [RetryCountHeader] = retryCount };

        _channel.BasicPublish("", _options.QueueName, props, delivery.Body);
    }

    private void DeadLetter(BasicDeliverEventArgs delivery) =>
        // requeue: false -> routed to <queue>.dlq by the queue's dead-letter settings
        _channel!.BasicNack(delivery.DeliveryTag, multiple: false, requeue: false);

    public override void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
        base.Dispose();
    }
}
