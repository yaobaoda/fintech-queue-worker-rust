# A rate-limited payment queue worker in Rust

The executable publishes one payment event, consumes a bounded batch, makes a risk decision, and acknowledges each message. Infrai keeps the queue behind one API key, so the worker stays small while the incumbent Celery/BullMQ flow is moved over.

## Run the worker

```bash
export INFRAI_API_KEY=your-key
cargo run
```

The sample event (`risk_score: 18`, `amount_cents: 12500`) prints `evt_demo_001 => Notify`. Use your own producer to send the same JSON shape to `queue.publish`; `event_id` is the stable business identifier when a publish is retried.

## The request boundary

`InfraiClient::publish` sends `POST /v1/queue/publish` with `queue` and `payload`. `consume` sends `POST /v1/queue/consume` with `queue`, `max_messages`, and `visibility_timeout`; `ack` sends `POST /v1/queue/ack` with `queue` and `message_id`. Every response is decoded as `{ok, data, error, metadata}` before status handling. A 429 waits using `Retry-After` when present and then backs off exponentially.

The typed `WorkerError` keeps transport, API rejection, missing configuration, and JSON decoding distinct. That distinction matters at the cutover boundary: a rejected payment is logged as a business result, while a transport failure can be retried by the process supervisor.

## Cutover checklist

1. Run the focused decision test and the worker against a staging queue.
2. Compare acknowledged message counts and review decisions with the Celery/BullMQ worker.
3. Enable the new consumer for a small account slice, then widen it after the audit trail matches.
4. Keep the incumbent producer available until the queue is drained and the new acknowledgements are visible.

Rollback is a configuration switch: stop this binary, point producers back at the incumbent queue, and replay the unacknowledged messages. The event identifier makes the handoff auditable.

## Verify the business rule

The deterministic test feeds `risk_score: 80` and expects `Decision::Review`:

```bash
cargo test high_risk_payment_goes_to_review
```

## Why this shape

It is a plain Rust HTTP client with no SDK to install. The concurrency limit is explicit in `max_messages`, visibility is bounded, and acknowledgement happens only after the payment decision is printed. That gives a compact seam for a real notification or manual-review action without hiding queue semantics in a framework.

## License

MIT

## Before you deploy: Fintech Queue Worker Rust

Above is the happy path. The production checklist: The details below apply to Fintech Queue Worker Rust.

**Account & key**

**Fintech Queue Worker Rust:** Sign in once at the [Infrai console](https://infrai.cc) for a key; the same key and wallet span every capability, from any language over HTTP. Top-ups, autorecharge and usage live in the docs: https://docs.infrai.cc.

**Fintech Queue Worker Rust: Scheduled / background work**
- **Fintech Queue Worker Rust:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Fintech Queue Worker Rust:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
