<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterface;
use RabbitEvents\Foundation\Message;

/**
 * A message in the outbox: the message to send, the key of the object it is about and when it expires.
 */
final class PendingMessage
{
    public function __construct(
        public readonly Message $message,
        public readonly ?string $key,
        public readonly ?CarbonImmutable $expiresAt,
        public readonly CarbonImmutable $createdAt,
        private readonly MessageCodec $codec,
        public readonly int|string|null $id = null,
        public readonly int $attempts = 0,
        public readonly ?string $lastError = null,
        public readonly ?CarbonImmutable $expiredAt = null,
    ) {
    }

    public function messageId(): ?string
    {
        return $this->message->id();
    }

    public function event(): string
    {
        return $this->message->event;
    }

    /**
     * The payload as a listener receives it (decoded from the message body).
     */
    public function payload(): mixed
    {
        return $this->codec->payload($this->message);
    }

    /**
     * The same message (its id, event and properties) with another payload, e.g. two payloads merged.
     */
    public function withPayload(mixed $payload): self
    {
        return $this->with(message: $this->codec->withPayload($this->message, $payload));
    }

    public function withExpiresAt(?CarbonImmutable $expiresAt): self
    {
        return $this->with(expiresAt: $expiresAt);
    }

    public function isExpired(CarbonInterface $now): bool
    {
        return $this->expiresAt !== null && $this->expiresAt->lessThanOrEqualTo($now);
    }

    /**
     * @internal Used by the stores.
     */
    public function stored(int|string $id): self
    {
        return $this->with(id: $id);
    }

    private function with(mixed ...$changes): self
    {
        return new self(...array_merge([
            'message' => $this->message,
            'key' => $this->key,
            'expiresAt' => $this->expiresAt,
            'createdAt' => $this->createdAt,
            'codec' => $this->codec,
            'id' => $this->id,
            'attempts' => $this->attempts,
            'lastError' => $this->lastError,
            'expiredAt' => $this->expiredAt,
        ], $changes));
    }
}
