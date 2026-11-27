using Payments.Core.Domain;
using Payments.Core.Services;

namespace Payments.Api.Endpoints;

public static class PaymentEndpoints
{
    private const string IdempotencyHeader = "Idempotency-Key";
    private const string MerchantHeader = "X-Merchant-Id"; // stands in for real authentication

    public static void MapPaymentEndpoints(this WebApplication app)
    {
        app.MapPost("/payments", CreatePayment);
        app.MapGet("/payments/{id:guid}", GetPayment);
    }

    private static async Task<IResult> CreatePayment(
        HttpRequest http, HttpResponse response, CreatePaymentRequest body,
        PaymentService service, CancellationToken ct)
    {
        var merchantId = http.Headers[MerchantHeader].ToString();
        var key = http.Headers[IdempotencyHeader].ToString();

        if (string.IsNullOrWhiteSpace(merchantId))
            return Results.Problem($"Missing {MerchantHeader} header.", statusCode: StatusCodes.Status401Unauthorized);
        if (string.IsNullOrWhiteSpace(key) || key.Length > 128)
            return Results.Problem($"A {IdempotencyHeader} header (max 128 chars) is required.",
                statusCode: StatusCodes.Status400BadRequest);
        if (body.Amount <= 0 || decimal.Round(body.Amount, 2) != body.Amount
            || string.IsNullOrWhiteSpace(body.Currency) || body.Currency.Trim().Length != 3
            || string.IsNullOrWhiteSpace(body.CardToken) || body.CardToken.Trim().Length > 128)
            return Results.ValidationProblem(new Dictionary<string, string[]>
            {
                ["request"] = ["Amount must be positive with at most 2 decimals, currency a 3-letter code, " +
                               "cardToken required (max 128 chars)."]
            });
        if (merchantId.Length > 64)
            return Results.Problem($"{MerchantHeader} header is too long (max 64 chars).",
                statusCode: StatusCodes.Status400BadRequest);

        var result = await service.CreateAsync(merchantId, key, body, ct);

        switch (result.Outcome)
        {
            case CreatePaymentOutcome.Created:
                return Results.Created($"/payments/{result.Payment!.Id}", ToDto(result.Payment));
            case CreatePaymentOutcome.Replayed:
                response.Headers["Idempotent-Replayed"] = "true";
                return Results.Ok(ToDto(result.Payment!));
            default:
                return Results.Problem(
                    $"{IdempotencyHeader} was already used with a different request body.",
                    statusCode: StatusCodes.Status422UnprocessableEntity);
        }
    }

    private static async Task<IResult> GetPayment(
        Guid id, HttpRequest http, PaymentService service, CancellationToken ct)
    {
        var merchantId = http.Headers[MerchantHeader].ToString();
        var payment = await service.GetAsync(merchantId, id, ct);
        return payment is null ? Results.NotFound() : Results.Ok(ToDto(payment));
    }

    private static object ToDto(Payment p) => new
    {
        p.Id,
        p.Amount,
        p.Currency,
        Status = p.Status.ToString(),
        p.CreatedAt,
        p.UpdatedAt
    };
}
