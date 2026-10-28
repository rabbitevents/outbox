<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Unit;

use Carbon\CarbonImmutable;
use RabbitEvents\Outbox\Contracts\Merger;
use RabbitEvents\Outbox\Events\OutboxDeliveryFailed;
use RabbitEvents\Outbox\Events\OutboxMessageMerged;
use RabbitEvents\Outbox\Events\OutboxMessageStored;
use RabbitEvents\Outbox\PendingMessage;
use RabbitEvents\Outbox\Tests\TestCase;
use RabbitEvents\Publisher\Events\MessagePublished;

class OutboxTest extends TestCase
{
    public function testPublishedMessageIsStoredInsteadOfSent(): void
    {
        $id = $this->publisher()->publish(new OrderUpdated(['id' => 1]));

        self::assertSame([], $this->direct->sent);
        self::assertCount(1, $rows = $this->rows());
        self::assertSame($id, $rows[0]->message_id);
        self::assertSame('order.updated', $rows[0]->event);
        self::assertSame(0, (int) $rows[0]->attempts);
        self::assertCount(1, $this->dispatched(OutboxMessageStored::class));
        self::assertSame([], $this->dispatched(MessagePublished::class), 'Not sent yet');
    }

    public function testMessageIsWrittenInTheTransactionOfTheEvent(): void
    {
        $publisher = $this->publisher();

        try {
            $this->db->transaction(function () use ($publisher) {
                $publisher->publish(new OrderUpdated());

                throw new \RuntimeException('Rolled back');
            });
        } catch (\RuntimeException) {
        }

        self::assertSame([], $this->rows(), 'A rolled back transaction leaves no message');

        $this->db->transaction(fn() => $publisher->publish(new OrderUpdated()));

        self::assertCount(1, $this->rows());
    }

    public function testStoredMessageIsSentRightAfterTheCommit(): void
    {
        $publisher = $this->publisher(['deliver_immediately' => true]);

        $this->db->transaction(function () use ($publisher) {
            $publisher->publish(new OrderUpdated());

            self::assertSame([], $this->transport->sent, 'Not before the commit');
        });

        self::assertCount(1, $this->transport->sent);
        self::assertSame([], $this->rows());
        self::assertCount(1, $this->dispatched(MessagePublished::class));
    }

    public function testMessagePublishedOutsideATransactionIsSentAtOnce(): void
    {
        $id = $this->publisher(['deliver_immediately' => true])->publish(new OrderUpdated());

        self::assertSame($id, $this->transport->sent[0]->id());
        self::assertSame([], $this->rows());
    }

    public function testMessageThatCannotBeSentRightAwayWaitsForTheRelay(): void
    {
        $this->transport->failWith = new \RuntimeException('Broker unavailable');

        // The message is stored: the publishing code doesn't see the failure.
        $id = $this->publisher(['deliver_immediately' => true])->publish(new OrderUpdated());

        self::assertCount(1, $rows = $this->rows());
        self::assertSame($id, $rows[0]->message_id);
        self::assertSame(1, (int) $rows[0]->attempts);
        self::assertSame('RuntimeException: Broker unavailable', $rows[0]->last_error);
        self::assertSame('2026-10-28 12:00:01', $this->date($rows[0]->available_at));
        self::assertCount(1, $this->dispatched(OutboxDeliveryFailed::class));
    }

    public function testAfterCommitEventReachesTheOutboxAfterTheCommit(): void
    {
        $transactions = new class {
            /** @var list<callable> */
            public array $callbacks = [];

            public function addCallback(callable $callback): void
            {
                $this->callbacks[] = $callback;
            }
        };
        $publisher = $this->publisher()->setTransactionManagerResolver(fn() => $transactions);
        $event = new OrderUpdated();
        $event->afterCommit = true;

        $publisher->publish($event);
        self::assertSame([], $this->rows());

        ($transactions->callbacks[0])();
        self::assertCount(1, $this->rows());
    }

    /**
     * @return array<string, array{0: OrderUpdated, 1: array<string, mixed>}>
     */
    public static function directEvents(): array
    {
        return [
            'ShouldPublishDirectly' => [new OrderViewed(), []],
            'except' => [new OrderUpdated(event: 'metrics.recorded'), ['except' => ['metrics.*']]],
            'disabled' => [new OrderUpdated(), ['enabled' => false]],
        ];
    }

    /**
     * @param array<string, mixed> $config
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('directEvents')]
    public function testEventPublishedDirectly(OrderUpdated $event, array $config): void
    {
        $id = $this->publisher($config)->publish($event);

        self::assertSame($id, $this->direct->sent[0]->id());
        self::assertSame([], $this->rows());
    }

    /**
     * @return array<string, array{0: OrderUpdated, 1: array<string, mixed>, 2: ?string}>
     */
    public static function timesToLive(): array
    {
        return [
            'configured' => [new OrderUpdated(), ['ttl' => 600], '2026-10-28 12:10:00'],
            'default: one hour' => [new OrderUpdated(), [], '2026-10-28 13:00:00'],
            'of the event' => [new KeyedOrderUpdated(ttl: 60), ['ttl' => 600], '2026-10-28 12:01:00'],
            'event without its own' => [new KeyedOrderUpdated(ttl: null), ['ttl' => 600], '2026-10-28 12:10:00'],
            'never expires' => [new KeyedOrderUpdated(ttl: 0), [], null],
            'never expires by config' => [new OrderUpdated(), ['ttl' => 0], null],
        ];
    }

    /**
     * @param array<string, mixed> $config
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('timesToLive')]
    public function testTimeToLive(OrderUpdated $event, array $config, ?string $expiresAt): void
    {
        $this->publisher($config)->publish($event);

        self::assertSame($expiresAt, $this->date($this->rows()[0]->expires_at));
    }

    public function testMessagesOfOneKeyAreMergedAndTheLatestWins(): void
    {
        $publisher = $this->publisher();

        $publisher->publish(new KeyedOrderUpdated(['id' => 1, 'status' => 'paid']));
        $second = $publisher->publish(new KeyedOrderUpdated(['id' => 1, 'status' => 'shipped']));

        self::assertCount(1, $rows = $this->rows());
        self::assertSame($second, $rows[0]->message_id);
        self::assertSame('order:1', $rows[0]->pending_key);
        $this->relay();
        self::assertSame([['id' => 1, 'status' => 'shipped']], $this->sentPayloads());

        [$merged] = $this->dispatched(OutboxMessageMerged::class);
        self::assertSame(['id' => 1, 'status' => 'paid'], $merged->pending->payload());
        self::assertSame($second, $merged->result->messageId());
    }

    public function testMessagesWithoutKeyOrWithAnotherKeyAreNotMerged(): void
    {
        $publisher = $this->publisher();

        $publisher->publish(new KeyedOrderUpdated(['id' => 1], key: null));
        $publisher->publish(new KeyedOrderUpdated(['id' => 1], key: null));
        $publisher->publish(new KeyedOrderUpdated(['id' => 1], key: 'order:1'));
        $publisher->publish(new KeyedOrderUpdated(['id' => 2], key: 'order:2'));

        self::assertCount(4, $this->rows());
    }

    public function testEventMergesItsOwnMessages(): void
    {
        $publisher = $this->publisher();

        $publisher->publish(new AccumulatingOrderUpdated(['status' => 'paid']));
        $last = $publisher->publish(new AccumulatingOrderUpdated(['tracking' => 'X1']));

        $this->relay();
        self::assertSame([['status' => 'paid', 'tracking' => 'X1']], $this->sentPayloads());
        self::assertSame($last, $this->transport->sent[0]->id());
    }

    public function testBoundMergerIsUsed(): void
    {
        $this->app->instance(Merger::class, new class implements Merger {
            public function merge(PendingMessage $pending, PendingMessage $new): PendingMessage
            {
                return $pending; // the first message wins
            }
        });
        $publisher = $this->publisher();

        $first = $publisher->publish(new KeyedOrderUpdated(['status' => 'paid']));
        $publisher->publish(new KeyedOrderUpdated(['status' => 'shipped']));

        $this->relay();
        self::assertSame($first, $this->transport->sent[0]->id());
    }

    public function testMessageOfAKeyIsStoredAgainOnceTheWaitingOneWasSent(): void
    {
        $publisher = $this->publisher();

        $publisher->publish(new KeyedOrderUpdated(['status' => 'paid']));
        $this->relay();
        $publisher->publish(new KeyedOrderUpdated(['status' => 'shipped']));

        self::assertCount(1, $rows = $this->rows());
        self::assertSame('order:1', $rows[0]->pending_key);
        $this->relay();
        self::assertSame([['status' => 'paid'], ['status' => 'shipped']], $this->sentPayloads());
    }

    public function testMessageOfAKeyIsStoredAgainOnceTheWaitingOneExpired(): void
    {
        $publisher = $this->publisher();

        $publisher->publish(new KeyedOrderUpdated(['status' => 'paid'], ttl: 60));
        CarbonImmutable::setTestNow('2026-10-28 12:05:00');
        $this->relay();
        $publisher->publish(new KeyedOrderUpdated(['status' => 'shipped'], ttl: 60));

        self::assertCount(2, $this->rows());
        $this->relay();
        self::assertSame([['status' => 'shipped']], $this->sentPayloads());
    }

    public function testMergedMessageKeepsTheTimeToLiveTheMergerGives(): void
    {
        $publisher = $this->publisher();

        $publisher->publish(new KeyedOrderUpdated(['status' => 'paid'], ttl: 60));
        CarbonImmutable::setTestNow('2026-10-28 12:00:30');
        $publisher->publish(new KeyedOrderUpdated(['status' => 'shipped'], ttl: 60));

        // Latest wins: the new message and its expiry.
        self::assertSame('2026-10-28 12:01:30', $this->date($this->rows()[0]->expires_at));
    }

    private function relay(): void
    {
        $this->deliverer()->deliver(100);
    }

    /**
     * @return list<mixed>
     */
    private function sentPayloads(): array
    {
        return array_map(
            fn($message) => json_decode($message->transportMessage()->getBody(), true),
            $this->transport->sent
        );
    }
}
