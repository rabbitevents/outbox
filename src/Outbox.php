<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Str;
use RabbitEvents\Foundation\Message;
use RabbitEvents\Outbox\Contracts\Merger;
use RabbitEvents\Outbox\Contracts\OutboxStore;
use RabbitEvents\Outbox\Events\OutboxMessageMerged;
use RabbitEvents\Outbox\Events\OutboxMessageStored;
use RabbitEvents\Publisher\Contracts\Outbox as OutboxContract;
use RabbitEvents\Publisher\ShouldPublish;
use Throwable;

/**
 * Takes the messages the publisher publishes and stores them for delivery. An event may define:
 * - `outboxKey(): ?string`: the object the message is about; a message of the same key that waits for delivery is
 *   merged with it (see Merger);
 * - `outboxTtl(): ?int`: seconds the message may wait for delivery (null: the configured ttl, 0: never expires);
 * - `mergeInOutbox(PendingMessage $pending, PendingMessage $new): PendingMessage`: how to merge its messages.
 */
class Outbox implements OutboxContract
{
    /**
     * @param array{enabled?: bool, ttl?: int, except?: list<string>, deliver_immediately?: bool} $config
     */
    public function __construct(
        private readonly OutboxStore $store,
        private readonly Deliverer $deliverer,
        private readonly MessageCodec $codec,
        private readonly Container $container,
        private readonly Dispatcher $events,
        private readonly array $config = []
    ) {
    }

    public function store(ShouldPublish $event, Message $message): bool
    {
        if (!$this->accepts($event)) {
            return false;
        }

        $merged = null;
        $stored = $this->store->store(
            $this->pending($event, $message),
            function (PendingMessage $pending, PendingMessage $new) use ($event, &$merged) {
                $result = method_exists($event, 'mergeInOutbox')
                    ? $event->mergeInOutbox($pending, $new)
                    : $this->container->make(Merger::class)->merge($pending, $new);

                $merged = [$pending, $new];

                return $result;
            }
        );

        $this->events->dispatch($merged ? new OutboxMessageMerged($merged[0], $merged[1], $stored) : new OutboxMessageStored($stored));

        if ($this->config['deliver_immediately'] ?? true) {
            $this->store->afterCommit(fn() => $this->deliverNow($stored));
        }

        return true;
    }

    private function accepts(ShouldPublish $event): bool
    {
        if (!($this->config['enabled'] ?? true) || $event instanceof ShouldPublishDirectly) {
            return false;
        }

        return !Str::is($this->config['except'] ?? [], $event->publishEventKey());
    }

    private function pending(ShouldPublish $event, Message $message): PendingMessage
    {
        $now = CarbonImmutable::now();
        $ttl = method_exists($event, 'outboxTtl') ? $event->outboxTtl() : null;
        $ttl ??= (int) ($this->config['ttl'] ?? 3600);

        return new PendingMessage(
            $message,
            method_exists($event, 'outboxKey') ? $event->outboxKey() : null,
            $ttl > 0 ? $now->addSeconds($ttl) : null,
            $now,
            $this->codec
        );
    }

    /**
     * The message is stored: if it can't be sent now, the relay sends it, so nothing reaches the publishing code.
     */
    private function deliverNow(PendingMessage $message): void
    {
        if ($message->id === null) {
            return;
        }

        try {
            $this->deliverer->deliver(1, [$message->id]);
        } catch (Throwable $exception) {
            if (!$this->container->bound(ExceptionHandler::class)) {
                throw $exception;
            }

            $this->container->make(ExceptionHandler::class)->report($exception);
        }
    }
}
