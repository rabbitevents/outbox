<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Carbon\CarbonImmutable;
use Illuminate\Contracts\Events\Dispatcher;
use RabbitEvents\Foundation\Contracts\Transport;
use RabbitEvents\Outbox\Contracts\OutboxStore;
use RabbitEvents\Outbox\Events\OutboxDeliveryFailed;
use RabbitEvents\Outbox\Events\OutboxMessageExpired;
use RabbitEvents\Publisher\Events\MessagePublished;
use Throwable;

/**
 * Sends the messages waiting in the outbox: used by the relay, and right after a message is stored.
 */
class Deliverer
{
    /**
     * @param list<int> $backoff Seconds before the next try after each failed one; the last value repeats.
     */
    public function __construct(
        private readonly OutboxStore $store,
        private readonly Transport $transport,
        private readonly Dispatcher $events,
        private readonly array $backoff = [1, 5, 30, 60]
    ) {
    }

    /**
     * Send up to `$limit` messages (only `$ids`, if given). After the first message that can't be sent, the rest wait:
     * the broker is likely unreachable, and they would only use up their tries.
     *
     * @param list<int|string>|null $ids
     */
    public function deliver(int $limit, ?array $ids = null): DeliveryReport
    {
        $failed = false;

        $results = $this->store->deliver(function (PendingMessage $message) use (&$failed): ?Delivery {
            $now = CarbonImmutable::now();

            if ($message->isExpired($now)) {
                return Delivery::expired();
            }

            if ($failed) {
                return null;
            }

            try {
                $this->transport->send($message->message);

                return Delivery::sent();
            } catch (Throwable $exception) {
                $failed = true;

                return Delivery::failed($exception, $now->addSeconds($this->backoff($message->attempts + 1)));
            }
        }, $limit, $ids);

        // After the store applied the results: a listener sees the message sent, expired or rescheduled.
        foreach ($results as [$message, $delivery]) {
            $this->events->dispatch(match ($delivery->status) {
                DeliveryStatus::Sent => new MessagePublished($message->message),
                DeliveryStatus::Expired => new OutboxMessageExpired($message),
                DeliveryStatus::Failed => new OutboxDeliveryFailed($message, $delivery->exception(), $delivery->retryAt()),
            });
        }

        return new DeliveryReport($results);
    }

    private function backoff(int $attempts): int
    {
        if ($this->backoff === []) {
            return 0;
        }

        return $this->backoff[$attempts - 1] ?? $this->backoff[array_key_last($this->backoff)];
    }
}
