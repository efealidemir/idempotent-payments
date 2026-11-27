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
        try
        {
            if (!Guid.TryParse(delivery.BasicProperties.MessageId, out var messageId))
                throw new InvalidOperationException("Message without a valid MessageId");

            var message = JsonSerializer.Deserialize<PaymentCreated>(Encoding.UTF8.GetString(delivery.Body.Span))
                          ?? throw new InvalidOperationException("Empty message body");

            using var scope = _scopes.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<PaymentAuthorizationHandler>();
            await handler.HandleAsync(messageId, message, ct);

            _channel!.BasicAck(delivery.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Processing failed; sending message to the dead-letter queue");
            // requeue: false -> routed to <queue>.dlq by the queue's dead-letter settings
            _channel!.BasicNack(delivery.DeliveryTag, multiple: false, requeue: false);
        }
    }

    public override void Dispose()
    {
        _channel?.Dispose();
        _connection?.Dispose();
        base.Dispose();
    }
}
