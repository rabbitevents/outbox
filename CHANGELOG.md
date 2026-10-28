# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-10-28

The first release. It delivers [RabbitEvents](https://github.com/rabbitevents/rabbitevents) messages through a database outbox. Without it, `publish()` sends a message at once. If RabbitMQ can't be reached, the exception reaches your code and the message is not sent. An `afterCommit` event is lost if publishing fails after the commit. With the outbox, a published message is written to a table first, in the transaction of the event if there is one. From there it is sent right after the commit, or later by a relay, until it is sent or its time to live ends.

Requires PHP 8.2+, Laravel 11, 12 or 13, and `rabbitevents/publisher` 10.1 or later (the outbox hook). Installing the package enables it.

### Added
- **Transactional outbox**: every published event goes to the `rabbitevents_outbox` table. A rollback leaves no message. The same message (ID, timestamp, properties, body) is sent after the commit. If that fails, nothing reaches your code: the message waits for the relay.
- **`afterCommit` keeps its timing**: such an event reaches the outbox after the commit, as it is published now, and is then delivered like any other message.
- **Relay**: `php artisan rabbitevents:outbox:relay` (`--sleep`, `--batch`, `--memory`, `--once`) sends waiting messages. A message that fails is tried again after `backoff` (default `[1, 5, 30, 60]` seconds; the last value repeats). The relay exits with code 1 after a failed send, so that the process manager restarts it with a new connection. `SIGTERM` stops it after the batch it is sending.
- **Several relays on one table**: each message is locked while it is sent (`FOR UPDATE SKIP LOCKED`), so no two relays send the same message. Needs MySQL 8+, MariaDB 10.6+ or PostgreSQL 9.5+. On older databases, set `skip_locked` to `false` and run one relay.
- **Time to live**: a message that waited longer than its TTL is never sent, so that a stale message doesn't reach a listener. The default is one hour (`ttl`). An event can set its own with `outboxTtl()` (`0`: never expires).
- **Expired messages are kept**: `rabbitevents:outbox:expired` lists them, `rabbitevents:outbox:forget {id}` deletes one, and `rabbitevents:outbox:prune --hours=168` deletes old ones.
- **Coalescing**: messages with the same `outboxKey()` are merged while they wait, so that one object has at most one waiting message. By default the latest message wins. Define `mergeInOutbox()` in the event, or bind a `Contracts\Merger`, to merge them differently (`PendingMessage::withPayload()`).
- **Direct publishing**: events that implement `ShouldPublishDirectly`, or match the `except` patterns, skip the outbox. `RABBITEVENTS_OUTBOX_ENABLED=false` disables the outbox for all events.
- **Events**: `OutboxMessageStored`, `OutboxMessageMerged`, `OutboxDeliveryFailed`, `OutboxMessageExpired`, and the publisher's `MessagePublished` when the outbox sends a message.
- **Custom stores**: `Contracts\OutboxStore`, implemented by `DatabaseOutboxStore`.
- **Testing**: `PublishableEventTesting::fake()` of the publisher replaces publishing, outbox included.

### Guarantees
- **At least once**: a relay that dies between sending a message and removing its row sends it again, with the same message ID. Use [`rabbitevents/deduplication`](https://github.com/rabbitevents/deduplication) or idempotent listeners.
- **Order**: messages of one key keep their order. Messages of different objects may arrive in another order, because transactions commit in their own order.
- **Timezones**: dates are stored as Unix timestamps, so the relay and the publishing processes may run with different timezones.
- **Sent** means written to the connection to RabbitMQ: RabbitEvents doesn't use publisher confirms.

### Tested on
Unit tests on SQLite with PHP 8.2 to 8.4. Integration tests on MySQL 8.4 and PostgreSQL 17 with a real RabbitMQ, on PHP 8.4. Not yet tested: MariaDB, MySQL before 8.0, `enqueue/amqp-ext`, and the Redis transport.

[0.1.0]: https://github.com/rabbitevents/outbox/releases/tag/v0.1.0
