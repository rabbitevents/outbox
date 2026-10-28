<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use RabbitEvents\Outbox\Contracts\Merger;

/**
 * The new message replaces the waiting one: the object's latest state is delivered.
 */
class LatestWins implements Merger
{
    public function merge(PendingMessage $pending, PendingMessage $new): PendingMessage
    {
        return $new;
    }
}
