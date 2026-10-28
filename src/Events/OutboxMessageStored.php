<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Events;

use RabbitEvents\Outbox\PendingMessage;

class OutboxMessageStored
{
    public function __construct(public readonly PendingMessage $message)
    {
    }
}
