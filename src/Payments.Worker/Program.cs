using Microsoft.EntityFrameworkCore;
using Payments.Core.Data;
using Payments.Core.Messaging;
using Payments.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<PaymentsDbContext>(o =>
    o.UseSqlServer(builder.Configuration.GetConnectionString("Payments")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection("RabbitMq"));
builder.Services.AddSingleton<IBankAuthorizer, FakeBankAuthorizer>();
builder.Services.AddScoped<PaymentAuthorizationHandler>();
builder.Services.AddHostedService<PaymentCreatedConsumer>();

builder.Build().Run();
