<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use RabbitEvents\Publisher\ShouldPublish;

/**
 * An event published directly, without the outbox (e.g. metrics that are worthless a moment later).
 */
interface ShouldPublishDirectly extends ShouldPublish
{
}
