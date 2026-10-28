# RabbitEvents Outbox

[![Tests](https://github.com/rabbitevents/outbox/actions/workflows/testing.yml/badge.svg)](https://github.com/rabbitevents/outbox/actions/workflows/testing.yml)
[![Static code analysis](https://github.com/rabbitevents/outbox/actions/workflows/static-code-analysis.yml/badge.svg)](https://github.com/rabbitevents/outbox/actions/workflows/static-code-analysis.yml)
[![Total Downloads](https://img.shields.io/packagist/dt/rabbitevents/outbox)](https://packagist.org/packages/rabbitevents/outbox)
[![Latest Version](https://img.shields.io/packagist/v/rabbitevents/outbox)](https://packagist.org/packages/rabbitevents/outbox)
[![License](https://img.shields.io/packagist/l/rabbitevents/outbox)](LICENSE)

Delivers [RabbitEvents](https://github.com/rabbitevents/rabbitevents) messages through a database outbox. A published message is written to a table, in the transaction of the event when there is one, and sent from there: right after the commit, and by a relay until it is sent or its time to live is over.

Without it, `publish()` sends the message right away: if RabbitMQ can't be reached, the exception reaches your code and the message isn't sent; an `afterCommit` event is lost if publishing fails after the commit.

- [Installation](#installation)
- [How It Works](#how-it-works)
- [Guarantees](#guarantees)
- [Time to Live](#time-to-live)
- [Coalescing the Messages of One Object](#coalescing)
- [Events Published Directly](#direct)
- [The Relay](#relay)
- [Expired Messages](#expired)
- [Events](#events)
- [Configuration](#configuration)
- [Custom Store](#custom-store)

## Installation <a name="installation"></a>

Requires `rabbitevents/publisher` 10.1 or later.

```bash
composer require rabbitevents/outbox
php artisan vendor:publish --tag=rabbitevents-outbox-migrations
php artisan migrate
```

Installing the package enables it: every published event goes through the outbox. Run the relay as a long-running process, like `rabbitevents:listen` (see [The Relay](#relay)):

```bash
php artisan rabbitevents:outbox:relay
```

To change the defaults, publish the config: `php artisan vendor:publish --tag=rabbitevents-outbox-config`.

## How It Works <a name="how-it-works"></a>

1. `publish()` writes the message to the `rabbitevents_outbox` table and returns its ID. Inside a database transaction the row is part of that transaction: a rollback leaves no message.
2. After the commit (at once, outside a transaction) the same process tries to send it. If that works, the row is removed. If it doesn't, the row stays, and nothing reaches your code: the message is safe in the table.
3. The relay takes the waiting messages in the order they were stored:
   - it sends a message and removes its row;
   - if sending fails, the message is tried again later: after 1, 5, 30 and then every 60 seconds by default (`backoff`);
   - a message whose time to live is over is never sent: it stays in the table as expired.

So every message is either sent or kept as expired.

## Guarantees <a name="guarantees"></a>

- **Atomic with your transaction**, when you publish inside a transaction on the outbox connection (`connection`, the default connection by default). A message written on another connection, or to a store that isn't your database, is not part of your transaction: the outbox still keeps it until it is sent.
- **`afterCommit` keeps its timing**: such an event reaches the outbox after the commit, as it is published now. It is then delivered like any other message. Only a process that dies between the commit and the write loses it.
- **At least once.** A relay that dies between sending a message and removing its row sends it again, with the same message ID. On the listener side, [`rabbitevents/deduplication`](https://github.com/rabbitevents/deduplication) skips such redeliveries; otherwise make your listeners idempotent.
- **Order**: messages are tried in the order they were stored, but transactions commit in their own order, so messages of different objects may arrive in another order. Messages of one object (one [key](#coalescing)) keep their order: an object has at most one waiting message.
- **Sent** means written to the connection to RabbitMQ: RabbitEvents doesn't use publisher confirms.

## Time to Live <a name="time-to-live"></a>

A message that waited longer than its time to live is never sent, so that a stale message can't reach a listener after the world has moved on. The default is one hour (`ttl`, in seconds). An event can set its own:

```php
use RabbitEvents\Publisher\ShouldPublish;
use RabbitEvents\Publisher\Support\Publishable;

class PriceChanged implements ShouldPublish
{
    use Publishable;

    public function __construct(private int $productId, private int $price)
    {
    }

    public function publishEventKey(): string
    {
        return 'price.changed';
    }

    public function toPublish(): mixed
    {
        return ['product_id' => $this->productId, 'price' => $this->price];
    }

    // Seconds; null: the configured ttl; 0: never expires.
    public function outboxTtl(): ?int
    {
        return 300;
    }
}
```

## Coalescing the Messages of One Object <a name="coalescing"></a>

While a message about an object waits for delivery (RabbitMQ is down, or the transaction hasn't committed yet), a new message about the same object can replace it, so that listeners don't get two messages in a row about one object. Only you know which messages are about one object: give the event a key.

```php
class OrderStatusChanged implements ShouldPublish
{
    use Publishable;

    public function __construct(private int $orderId, private string $status)
    {
    }

    public function publishEventKey(): string
    {
        return 'order.status_changed';
    }

    public function toPublish(): mixed
    {
        return ['order_id' => $this->orderId, 'status' => $this->status];
    }

    // Messages with the same key are merged while they wait; null: never merged.
    public function outboxKey(): ?string
    {
        return "order:{$this->orderId}";
    }
}
```

By default the latest message wins: the waiting message is replaced by the new one (its ID, payload and time to live). A key holds only while a message waits: once it is sent or expired, the next message of the object waits on its own.

To merge messages differently, define `mergeInOutbox()` in the event, or bind your own `RabbitEvents\Outbox\Contracts\Merger` for all events. Both get the waiting message and the new one and return the message to deliver: one of them, or a new one built with `withPayload()`, which keeps the ID and the properties of the message it is called on.

```php
use RabbitEvents\Outbox\PendingMessage;

class OrderChanged implements ShouldPublish
{
    use Publishable;

    /**
     * @param array<string, mixed> $changes
     */
    public function __construct(private int $orderId, private array $changes)
    {
    }

    public function publishEventKey(): string
    {
        return 'order.changed';
    }

    public function toPublish(): mixed
    {
        return $this->changes;
    }

    public function outboxKey(): ?string
    {
        return "order:{$this->orderId}";
    }

    // One message with all the changes that waited.
    public function mergeInOutbox(PendingMessage $pending, PendingMessage $new): PendingMessage
    {
        return $new->withPayload(array_merge($pending->payload(), $new->payload()));
    }
}
```

`payload()` returns the payload as a listener receives it. `$pending->createdAt`, `$pending->expiresAt` and `$pending->attempts` tell how long and how often the waiting message was tried; `withExpiresAt()` changes the expiry of the result.

## Events Published Directly <a name="direct"></a>

Some messages are worthless a moment later (metrics, presence): publish them directly, without the outbox, by implementing `RabbitEvents\Outbox\ShouldPublishDirectly` instead of `ShouldPublish`, or by listing their names in `except` (patterns as in `Str::is()`, e.g. `metrics.*`). `RABBITEVENTS_OUTBOX_ENABLED=false` disables the outbox for all events.

## The Relay <a name="relay"></a>

`php artisan rabbitevents:outbox:relay` sends the waiting messages. **If no relay runs, a message that couldn't be sent right after the commit waits until it expires.** Keep it running with your process manager, like the listener worker:

```ini
[program:rabbitevents-outbox-relay]
command=php /var/www/html/artisan rabbitevents:outbox:relay
autostart=true
autorestart=true
startsecs=1
stopwaitsecs=10
```

- Options: `--sleep=1` (seconds to wait when nothing is due), `--batch=100`, `--memory=128` (MB, then it exits for a restart), `--once` (one batch, then exit).
- When a message can't be sent, the relay exits with code 1: its connection to RabbitMQ may be broken for good, and a new process gets a new one. Restart it with a delay (`startsecs`, `RestartSec`). The message waits for its next try.
- `SIGTERM` stops it after the batch it is sending (at once while it waits).
- Several relays can run on one table: each message is locked while it is sent (`FOR UPDATE SKIP LOCKED`), so no two relays send the same message and none waits for another. This needs MySQL 8+, MariaDB 10.6+ or PostgreSQL 9.5+; on older databases set `skip_locked` to `false` and run a single relay. SQLite needs no locks (one writer at a time).
- Instead of a long-running process you can schedule `rabbitevents:outbox:relay --once` (e.g. `everyFiveSeconds()`), at the cost of a longer delay for messages that failed right after the commit.

## Expired Messages <a name="expired"></a>

Expired messages stay in the table with `expired_at`, so you can see what was never sent. The dates of the table are Unix timestamps, so the relay and the publishing processes may run with different timezones; the commands show them in the timezone of the application:

```bash
php artisan rabbitevents:outbox:expired          # list them: ID, message ID, event, key, attempts, last error
php artisan rabbitevents:outbox:forget 42        # delete one
php artisan rabbitevents:outbox:prune --hours=168 # delete those expired more than a week ago
```

Schedule `rabbitevents:outbox:prune` so that the table doesn't grow.

## Events <a name="events"></a>

| Event | When |
|---|---|
| `RabbitEvents\Outbox\Events\OutboxMessageStored` | A message was written to the outbox. |
| `RabbitEvents\Outbox\Events\OutboxMessageMerged` | A message was merged with the waiting message of its key (`$pending`, `$new`, `$result`). |
| `RabbitEvents\Publisher\Events\MessagePublished` | A message was sent, right after the commit or by the relay (the same event the publisher dispatches without the outbox). |
| `RabbitEvents\Outbox\Events\OutboxDeliveryFailed` | Sending failed; `$exception`, and `$retryAt` for the next try. |
| `RabbitEvents\Outbox\Events\OutboxMessageExpired` | A message expired and will never be sent. |

## Configuration <a name="configuration"></a>

`config/rabbitevents-outbox.php`:

| Key | Env | Default | |
|---|---|---|---|
| `enabled` | `RABBITEVENTS_OUTBOX_ENABLED` | `true` | `false`: every event is published directly. |
| `connection` | `RABBITEVENTS_OUTBOX_CONNECTION` | default connection | Use the connection of your business transactions. |
| `table` | `RABBITEVENTS_OUTBOX_TABLE` | `rabbitevents_outbox` | |
| `ttl` | `RABBITEVENTS_OUTBOX_TTL` | `3600` | Seconds; `0`: never expires. |
| `except` | | `[]` | Events published directly. |
| `deliver_immediately` | `RABBITEVENTS_OUTBOX_DELIVER_IMMEDIATELY` | `true` | `false`: only the relay sends messages. |
| `backoff` | | `[1, 5, 30, 60]` | Seconds before the next try; the last value repeats. |
| `skip_locked` | `RABBITEVENTS_OUTBOX_SKIP_LOCKED` | `true` | See [The Relay](#relay). |

## Custom Store <a name="custom-store"></a>

The messages are kept by `RabbitEvents\Outbox\Contracts\OutboxStore`, implemented by `DatabaseOutboxStore`. To keep them elsewhere (e.g. MongoDB), bind your own implementation; it must keep messages as durably as your data, and only a store in your database makes a message part of your transaction.

## Testing

`PublishableEventTesting::fake()` of the publisher replaces publishing, outbox included: `assertPublished()` works as without the outbox, and nothing is written to the table.

## License

MIT, see [LICENSE](LICENSE).
