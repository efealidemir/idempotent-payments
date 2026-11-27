# Idempotent Payments API (.NET)

[![CI](https://github.com/efealidemir/idempotent-payments/actions/workflows/ci.yml/badge.svg)](https://github.com/efealidemir/idempotent-payments/actions/workflows/ci.yml)

A small payment-processing service that shows how to accept payment requests **exactly once**, even when clients retry, the network fails or messages are delivered twice.

> Status: work in progress. The solution builds with no errors or warnings on .NET 8 and the unit tests pass; the Docker Compose stack (SQL Server + RabbitMQ) has not been run end to end yet.

## Why this project

Payment APIs have to survive retries: a mobile client times out and sends the same request again, a message broker redelivers a message, two instances race on the same request. This repo demonstrates the patterns that keep money movements correct in that world:

- **Idempotency keys** on the HTTP API (`Idempotency-Key` header, scoped per merchant, request hash checked).
- **Transactional outbox**: the payment row and the "payment created" event are saved in the same database transaction, then published to RabbitMQ by a background service.
- **Idempotent consumer**: the worker records every processed message id, so a redelivered message changes nothing.
- **Retries with exponential backoff** on the publisher side (2 s, 4 s, 8 s ... capped at 5 min); at-least-once delivery end to end.
- **Retry then dead-letter** on the consumer side: unreadable messages go straight to `payments.created.dlq`; transient failures are retried 3 times (`x-retry-count` header) before being dead-lettered.

## Architecture

```
client ──POST /payments (Idempotency-Key)──▶ Payments.Api
                                               │  one SQL transaction:
                                               │   • Payments row
                                               │   • OutboxMessages row
                                               ▼
                                          SQL Server
                                               ▲
             OutboxPublisher (BackgroundService) │ polls unprocessed rows
                                               ▼
                                          RabbitMQ  (queue: payments.created)
                                               ▼
                                         Payments.Worker
                                           • skips message ids already in ProcessedMessages
                                           • simulates bank authorization
                                           • updates payment status + ProcessedMessages in one transaction
```

| Project | Role |
| --- | --- |
| `src/Payments.Core` | Domain model, EF Core `PaymentsDbContext`, `PaymentService` (idempotency logic), RabbitMQ helpers |
| `src/Payments.Api` | Minimal API (`POST /payments`, `GET /payments/{id}`), outbox publisher |
| `src/Payments.Worker` | Consumer that authorizes payments idempotently |
| `tests/Payments.Tests` | xUnit tests: idempotency rules, the concurrent-request race (on SQLite, which enforces the unique index), retry policy |

Stack: .NET 8, ASP.NET Core minimal APIs, EF Core (SQL Server), RabbitMQ, Docker Compose, xUnit, GitHub Actions.

## Run it

```bash
docker compose up --build
```

Then:

```bash
curl -i -X POST http://localhost:8080/payments \
  -H "Content-Type: application/json" \
  -H "X-Merchant-Id: merchant-1" \
  -H "Idempotency-Key: 7f1c2a9e-0001" \
  -d '{"amount": 49.90, "currency": "EUR", "cardToken": "tok_visa_4242"}'
```

Send the same request again: you get the same payment back (`Idempotent-Replayed: true`), and no second payment is created. Send the same key with a different body: `422 Unprocessable Entity`.

RabbitMQ management UI: http://localhost:15672 (guest / guest).

## Tests

```bash
dotnet test
```

## Roadmap

1. **Week 1 — make it run:** `dotnet build`, `docker compose up`, the two curl calls above work, tests pass in CI.
2. **Week 2 — robustness (done):** handle the unique-index race (two identical requests in parallel), publisher retry with exponential backoff, dead-letter queue for poison messages.
3. **Week 3 — observability:** structured logs (Serilog), correlation id from HTTP request to worker, health checks for SQL Server and RabbitMQ.
4. **Week 4 — polish for recruiters:** integration tests with Testcontainers, a load test showing no duplicates under 1,000 concurrent retries, an architecture diagram, and a short write-up of the design decisions in this README.

Optional later: refunds (also idempotent), webhook notifications to merchants with signed payloads, deployment to Azure Container Apps.

## Design notes

- The idempotency key is unique **per merchant** (`MerchantId + IdempotencyKey` unique index), so two merchants can use the same key.
- A SHA-256 hash of the normalized request body is stored with the key. Same key + different body = client bug → `422`, never a silent overwrite.
- The outbox is polled rather than using CDC to keep the demo dependency-free.
- Delivery is at-least-once; correctness comes from the idempotent consumer, not from the broker.
- **The race:** two identical requests can both pass the "does this key exist?" check. The database unique index lets only one insert win; the loser catches the violation, reloads the winner's row and returns it (`Replayed`), or `422` if the bodies differ. `ConcurrentRequestTests` reproduces this deterministically.
- On a consumer retry the message is republished *before* the original is acknowledged, so a crash can cause a duplicate but never a loss; the idempotent handler absorbs the duplicate.
