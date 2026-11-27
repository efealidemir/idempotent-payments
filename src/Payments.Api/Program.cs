using Microsoft.EntityFrameworkCore;
using Payments.Api;
using Payments.Api.Endpoints;
using Payments.Core.Data;
using Payments.Core.Messaging;
using Payments.Core.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<PaymentsDbContext>(o =>
    o.UseSqlServer(builder.Configuration.GetConnectionString("Payments")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddScoped<PaymentService>();
builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection("RabbitMq"));
builder.Services.AddHostedService<OutboxPublisher>();

var app = builder.Build();

// Demo convenience: create the schema on start-up. Replace with EF migrations (roadmap, week 2).
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
    await db.Database.EnsureCreatedAsync();
}

app.MapPaymentEndpoints();
app.MapGet("/health", () => Results.Ok("ok"));

app.Run();

public partial class Program; // for WebApplicationFactory in integration tests
