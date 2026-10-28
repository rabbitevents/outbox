<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Unit;

use RabbitEvents\Outbox\PendingMessage;
use RabbitEvents\Outbox\ShouldPublishDirectly;
use RabbitEvents\Publisher\ShouldPublish;

class OrderUpdated implements ShouldPublish
{
    public ?bool $afterCommit = null;

    public function __construct(public mixed $payload = ['id' => 1], public string $event = 'order.updated')
    {
    }

    public function publishEventKey(): string
    {
        return $this->event;
    }

    public function toPublish(): mixed
    {
        return $this->payload;
    }
}

/**
 * Messages about one order are coalesced; they live `$ttl` seconds.
 */
class KeyedOrderUpdated extends OrderUpdated
{
    public function __construct(mixed $payload = ['id' => 1], public ?int $ttl = null, public ?string $key = 'order:1')
    {
        parent::__construct($payload);
    }

    public function outboxKey(): ?string
    {
        return $this->key;
    }

    public function outboxTtl(): ?int
    {
        return $this->ttl;
    }
}

/**
 * Accumulates the changed fields of the order in one message.
 */
class AccumulatingOrderUpdated extends KeyedOrderUpdated
{
    public function mergeInOutbox(PendingMessage $pending, PendingMessage $new): PendingMessage
    {
        return $new->withPayload(array_merge($pending->payload(), $new->payload()));
    }
}

class OrderViewed extends OrderUpdated implements ShouldPublishDirectly
{
}

class JsonPayloadObject implements \JsonSerializable
{
    /**
     * @return array<string, int>
     */
    public function jsonSerialize(): array
    {
        return ['id' => 7];
    }
}
