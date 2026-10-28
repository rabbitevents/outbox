<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Contracts;

use Carbon\CarbonInterface;
use Closure;
use RabbitEvents\Outbox\Delivery;
use RabbitEvents\Outbox\PendingMessage;

/**
 * Where messages wait for delivery. DatabaseOutboxStore keeps them in a table of the connection of your business
 * transaction; another store must keep them as durably, and makes no message part of a database transaction.
 */
interface OutboxStore
{
    /**
     * Store the message. If a message of the same key waits for delivery, store what `$merge` returns instead.
     *
     * @param Closure(PendingMessage $pending, PendingMessage $new): PendingMessage $merge
     * @return PendingMessage The stored message, with its id.
     */
    public function store(PendingMessage $message, Closure $merge): PendingMessage;

    /**
     * Run the callback once what was stored is durable: after the current transaction commits, or at once.
     */
    public function afterCommit(Closure $callback): void;

    /**
     * Hand up to `$limit` messages waiting for delivery (in the order they were stored; only `$ids` if given) to
     * `$deliver`, and apply what it returns: sent messages are removed, expired ones are kept as expired, failed
     * ones wait until their next try. A message for which it returns null is left as it is. No other caller gets
     * these messages meanwhile.
     *
     * @param Closure(PendingMessage): ?Delivery $deliver
     * @param list<int|string>|null $ids
     * @return list<array{0: PendingMessage, 1: Delivery}>
     */
    public function deliver(Closure $deliver, int $limit, ?array $ids = null): array;

    /**
     * @return list<PendingMessage>
     */
    public function expired(int $limit): array;

    public function forget(int|string $id): bool;

    /**
     * Remove the messages that expired at or before the given time (dates are stored to the second).
     */
    public function prune(CarbonInterface $expiredBefore): int;
}
