<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Unit;

use Carbon\CarbonImmutable;
use RabbitEvents\Foundation\Message;
use RabbitEvents\Outbox\Events\OutboxDeliveryFailed;
use RabbitEvents\Outbox\Events\OutboxMessageExpired;
use RabbitEvents\Outbox\Events\OutboxMessageStored;
use RabbitEvents\Outbox\Tests\TestCase;
use RabbitEvents\Publisher\Events\MessagePublished;

class DelivererTest extends TestCase
{
    public function testMessagesAreSentInTheOrderTheyWereStoredAndRemoved(): void
    {
        $publisher = $this->publisher();
        $ids = [
            $publisher->publish(new OrderUpdated(['id' => 1])),
            $publisher->publish(new OrderUpdated(['id' => 2])),
            $publisher->publish(new OrderUpdated(['id' => 3])),
        ];

        $report = $this->deliverer()->deliver(100);

        self::assertSame($ids, array_map(fn(Message $message) => $message->id(), $this->transport->sent));
        self::assertSame(3, $report->total());
        self::assertSame([], $this->rows());
        self::assertSame($ids, array_map(
            fn(MessagePublished $event) => $event->message->id(),
            $this->dispatched(MessagePublished::class)
        ));
    }

    public function testBatchTakesAtMostItsLimit(): void
    {
        $publisher = $this->publisher();
        $publisher->publish(new OrderUpdated(['id' => 1]));
        $publisher->publish(new OrderUpdated(['id' => 2]));

        $this->deliverer()->deliver(1);

        self::assertCount(1, $this->transport->sent);
        self::assertCount(1, $this->rows());
    }

    /**
     * @return array<string, array{0: mixed}>
     */
    public static function payloads(): array
    {
        return [
            'associative array' => [['id' => 1, 'items' => [['sku' => 'A', 'qty' => 2]], 'total' => 9.5]],
            'list' => [[1, 'two', 3.0]],
            'empty array' => [[]],
            'scalar' => ['paid'],
            'null' => [null],
            'object' => [new JsonPayloadObject()],
            'unicode' => [['name' => 'Пётр', 'emoji' => "\u{1F680}"]],
        ];
    }

    /**
     * The relay sends exactly the message that was published: body, properties, id, timestamp, routing key.
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('payloads')]
    public function testSentMessageIsTheMessageThatWasPublished(mixed $payload): void
    {
        $this->publisher()->publish(new OrderUpdated($payload));
        [$stored] = $this->dispatched(OutboxMessageStored::class);
        $published = $stored->message->message->transportMessage();

        $this->deliverer()->deliver(100);
        $sent = $this->transport->sent[0]->transportMessage();

        self::assertSame($published->getBody(), $sent->getBody());
        self::assertSame($published->getProperties(), $sent->getProperties());
        self::assertSame($published->getMessageId(), $sent->getMessageId());
        self::assertSame($published->getTimestamp(), $sent->getTimestamp());
        self::assertSame($published->getRoutingKey(), $sent->getRoutingKey());
        self::assertEquals($published->getOrigin(), $sent->getOrigin());
        self::assertEquals(
            json_decode($published->getBody(), true),
            $this->transport->sent[0]->payload->value(),
            'The payload of a sent message can be read'
        );
    }

    public function testExpiredMessageIsKeptAndNeverSent(): void
    {
        $this->publisher()->publish(new KeyedOrderUpdated(ttl: 60));
        CarbonImmutable::setTestNow('2026-10-28 12:01:00');

        $this->deliverer()->deliver(100);
        $this->deliverer()->deliver(100);

        self::assertSame([], $this->transport->sent);
        self::assertCount(1, $rows = $this->rows());
        self::assertSame('2026-10-28 12:01:00', $this->date($rows[0]->expired_at));
        self::assertNull($rows[0]->pending_key, 'The key is free for new messages of the object');
        self::assertCount(1, $this->dispatched(OutboxMessageExpired::class));
    }

    public function testMessageThatCouldNotBeSentIsTriedAgainAfterItsBackoff(): void
    {
        $this->publisher()->publish(new OrderUpdated());
        $this->transport->failWith = new \RuntimeException('Broker unavailable');

        // backoff [1, 5, 30]: the last value repeats.
        foreach (['12:00:01', '12:00:06', '12:00:36', '12:01:06'] as $attempt => $retryAt) {
            $this->deliverer()->deliver(100);
            $row = $this->rows()[0];

            self::assertSame($attempt + 1, (int) $row->attempts);
            self::assertSame("2026-10-28 $retryAt", $this->date($row->available_at));

            // Not taken before its next try.
            CarbonImmutable::setTestNow(CarbonImmutable::createFromTimestamp((int) $row->available_at)->subSecond());
            self::assertSame(0, $this->deliverer()->deliver(100)->total());
            CarbonImmutable::setTestNow(CarbonImmutable::createFromTimestamp((int) $row->available_at));
        }

        $this->transport->failWith = null;
        $this->deliverer()->deliver(100);

        self::assertCount(1, $this->transport->sent);
        self::assertSame([], $this->rows());
        self::assertCount(4, $failures = $this->dispatched(OutboxDeliveryFailed::class));
        self::assertSame('Broker unavailable', $failures[0]->exception->getMessage());
    }

    public function testAfterAFailureTheRestOfTheBatchWaits(): void
    {
        $publisher = $this->publisher();
        $publisher->publish(new OrderUpdated(['id' => 1]));
        $publisher->publish(new OrderUpdated(['id' => 2]));
        $this->transport->failWith = new \RuntimeException('Broker unavailable');

        $report = $this->deliverer()->deliver(100);

        self::assertTrue($report->failed());
        $rows = $this->rows();
        self::assertSame(1, (int) $rows[0]->attempts);
        self::assertSame(0, (int) $rows[1]->attempts, 'Not tried: it would only use up a try');
        self::assertSame('2026-10-28 12:00:00', $this->date($rows[1]->available_at));
    }

    public function testExpiredMessagesAreExpiredEvenAfterAFailure(): void
    {
        $publisher = $this->publisher();
        $publisher->publish(new OrderUpdated(['id' => 1]));
        $publisher->publish(new KeyedOrderUpdated(['id' => 2], ttl: 30));
        CarbonImmutable::setTestNow('2026-10-28 12:00:30');
        $this->transport->failWith = new \RuntimeException('Broker unavailable');

        $this->deliverer()->deliver(100);

        self::assertNotNull($this->rows()[1]->expired_at);
    }

    public function testOnlyTheGivenMessagesAreTaken(): void
    {
        $publisher = $this->publisher();
        $publisher->publish(new OrderUpdated(['id' => 1]));
        $second = $publisher->publish(new OrderUpdated(['id' => 2]));
        $rowId = $this->rows()[1]->id;

        $this->deliverer()->deliver(100, [$rowId]);

        self::assertSame($second, $this->transport->sent[0]->id());
        self::assertCount(1, $this->rows());
    }

    public function testLongErrorIsCut(): void
    {
        $this->publisher()->publish(new OrderUpdated());
        $this->transport->failWith = new \RuntimeException(str_repeat('x', 10000));

        $this->deliverer()->deliver(100);

        self::assertSame(4096, strlen($this->rows()[0]->last_error));
    }
}
