<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

/**
 * What one delivery round did.
 */
final class DeliveryReport
{
    /**
     * @param list<array{0: PendingMessage, 1: Delivery}> $results
     */
    public function __construct(public readonly array $results)
    {
    }

    public function count(DeliveryStatus $status): int
    {
        return count(array_filter($this->results, fn(array $result) => $result[1]->status === $status));
    }

    /**
     * How many messages the round took; fewer than its limit means nothing else is due.
     */
    public function total(): int
    {
        return count($this->results);
    }

    public function failed(): bool
    {
        return $this->count(DeliveryStatus::Failed) > 0;
    }
}
