<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Events;

use Carbon\CarbonImmutable;
use RabbitEvents\Outbox\PendingMessage;
use Throwable;

/**
 * Sending a message failed; it is tried again at `$retryAt`, unless it expires before.
 */
class OutboxDeliveryFailed
{
    public function __construct(
        public readonly PendingMessage $message,
        public readonly Throwable $exception,
        public readonly CarbonImmutable $retryAt
    ) {
    }
}
