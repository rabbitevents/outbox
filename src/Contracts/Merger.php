<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Contracts;

use RabbitEvents\Outbox\PendingMessage;

/**
 * Merges a new message into the message of the same key that waits for delivery, so that one object doesn't get
 * two messages in a row. An event can merge its messages itself with `mergeInOutbox($pending, $new)`.
 */
interface Merger
{
    /**
     * @return PendingMessage The message to deliver instead of both: one of them, or a new one
     *                        (`$new->withPayload(...)`).
     */
    public function merge(PendingMessage $pending, PendingMessage $new): PendingMessage;
}
