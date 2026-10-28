<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use RabbitEvents\Foundation\Message;
use RabbitEvents\Foundation\Serialization\SerializerRegistry;

/**
 * Turns a message into what a store keeps and back, so that the relay sends exactly the message that was published:
 * the same body, properties, id and timestamp.
 */
final class MessageCodec
{
    public function __construct(private readonly SerializerRegistry $registry)
    {
    }

    /**
     * @return array{message_id: string, event: string, body: string, properties: string, timestamp: int|null}
     */
    public function encode(Message $message): array
    {
        $transportMessage = $message->transportMessage();

        return [
            'message_id' => (string) $message->id(),
            'event' => $message->event,
            // Base64: the body may be binary (Protobuf).
            'body' => base64_encode($transportMessage->getBody()),
            'properties' => json_encode($transportMessage->getProperties(), JSON_THROW_ON_ERROR),
            'timestamp' => $transportMessage->getTimestamp(),
        ];
    }

    public function decode(string $messageId, string $event, string $body, string $properties, ?int $timestamp): Message
    {
        $properties = json_decode($properties, true, 512, JSON_THROW_ON_ERROR);
        $payload = new StoredPayload(
            (string) base64_decode($body, true),
            (string) ($properties['content_type'] ?? 'application/json')
        );

        $message = new Message($event, $payload, ['message_id' => $messageId] + $properties);
        $payload->decodeWith(fn() => $this->payload($message));

        if ($timestamp) {
            $message->transportMessage()->setTimestamp($timestamp);
        }

        return $message;
    }

    public function payload(Message $message): mixed
    {
        $transportMessage = $message->transportMessage();

        return $this->registry
            ->get((string) $transportMessage->getProperty('content_type'))
            ->deserialize($transportMessage)
            ->value();
    }

    /**
     * The message with another payload; its id, event, timestamp and other properties are kept.
     */
    public function withPayload(Message $message, mixed $value): Message
    {
        $transportMessage = $message->transportMessage();
        $properties = $transportMessage->getProperties();
        unset($properties['type']);

        if (is_object($value)) {
            $properties['type'] = get_class($value);
        }

        $merged = new Message(
            $message->event,
            $this->registry->resolve($value)->serialize($value),
            ['message_id' => $message->id()] + $properties
        );

        if ($timestamp = $transportMessage->getTimestamp()) {
            $merged->transportMessage()->setTimestamp($timestamp);
        }

        return $merged;
    }
}
