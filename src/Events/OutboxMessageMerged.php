<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Events;

use RabbitEvents\Outbox\PendingMessage;

/**
 * A message was merged into the message of the same key that waited for delivery.
 */
class OutboxMessageMerged
{
    public function __construct(
        public readonly PendingMessage $pending,
        public readonly PendingMessage $new,
        public readonly PendingMessage $result
    ) {
    }
}
