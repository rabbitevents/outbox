<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Carbon\CarbonImmutable;
use LogicException;
use Throwable;

/**
 * What happened to a message the relay took.
 */
final class Delivery
{
    private function __construct(
        public readonly DeliveryStatus $status,
        private readonly ?Throwable $exception = null,
        private readonly ?CarbonImmutable $retryAt = null
    ) {
    }

    public static function sent(): self
    {
        return new self(DeliveryStatus::Sent);
    }

    public static function expired(): self
    {
        return new self(DeliveryStatus::Expired);
    }

    public static function failed(Throwable $exception, CarbonImmutable $retryAt): self
    {
        return new self(DeliveryStatus::Failed, $exception, $retryAt);
    }

    /**
     * Why sending the message failed.
     */
    public function exception(): Throwable
    {
        return $this->exception ?? throw new LogicException('The message did not fail.');
    }

    /**
     * When a failed message is tried again.
     */
    public function retryAt(): CarbonImmutable
    {
        return $this->retryAt ?? throw new LogicException('The message did not fail.');
    }
}
