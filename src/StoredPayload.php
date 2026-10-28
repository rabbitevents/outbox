<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Closure;
use RabbitEvents\Foundation\Contracts\ContentType;
use RabbitEvents\Foundation\Contracts\Payload;

/**
 * The payload of a message read from the outbox: sent as it was stored, decoded only when asked.
 */
final class StoredPayload implements Payload
{
    private ?Closure $decode = null;

    public function __construct(private readonly string $body, private readonly string $contentType)
    {
    }

    /**
     * @internal
     */
    public function decodeWith(Closure $decode): void
    {
        $this->decode = $decode;
    }

    public function value(): mixed
    {
        return $this->decode ? ($this->decode)() : $this->body;
    }

    public function serialize(): string
    {
        return $this->body;
    }

    public function contentType(): ContentType
    {
        return new class ($this->contentType) implements ContentType {
            public function __construct(private readonly string $value)
            {
            }

            public function getValue(): string
            {
                return $this->value;
            }

            public function __toString(): string
            {
                return $this->value;
            }
        };
    }
}
