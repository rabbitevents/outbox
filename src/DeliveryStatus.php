<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

enum DeliveryStatus: string
{
    case Sent = 'sent';
    case Expired = 'expired';
    case Failed = 'failed';
}
