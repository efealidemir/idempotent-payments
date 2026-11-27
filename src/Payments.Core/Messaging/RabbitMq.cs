using RabbitMQ.Client;

namespace Payments.Core.Messaging;

public class RabbitMqOptions
{
    public string HostName { get; set; } = "localhost";
    public string UserName { get; set; } = "guest";
    public string Password { get; set; } = "guest";
    public string QueueName { get; set; } = "payments.created";
}

public static class RabbitMq
{
    public static IConnection Connect(RabbitMqOptions options) =>
        new ConnectionFactory
        {
            HostName = options.HostName,
            UserName = options.UserName,
            Password = options.Password,
            DispatchConsumersAsync = true,
            AutomaticRecoveryEnabled = true
        }.CreateConnection();

    /// <summary>Durable queue with a dead-letter queue for messages that keep failing.</summary>
    public static void DeclareTopology(IModel channel, string queueName)
    {
        var deadLetterQueue = queueName + ".dlq";
        channel.QueueDeclare(deadLetterQueue, durable: true, exclusive: false, autoDelete: false);
        channel.QueueDeclare(queueName, durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                ["x-dead-letter-exchange"] = "",
                ["x-dead-letter-routing-key"] = deadLetterQueue
            });
    }
}
