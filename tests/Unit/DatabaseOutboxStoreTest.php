<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Unit;

use Carbon\CarbonImmutable;
use PHPUnit\Framework\Attributes\DataProvider;
use RabbitEvents\Outbox\Tests\TestCase;

class DatabaseOutboxStoreTest extends TestCase
{
    public function testWithoutATransactionsManagerTheMessageIsLeftToTheRelay(): void
    {
        // Outside a Laravel application a connection may have no transactions manager: sending before the commit
        // could send a message that is then rolled back.
        $this->db->unsetTransactionManager();
        $publisher = $this->publisher(['deliver_immediately' => true]);

        $this->db->transaction(fn() => $publisher->publish(new OrderUpdated()));

        self::assertSame([], $this->transport->sent);
        self::assertCount(1, $this->rows());
    }

    public function testWithoutATransactionTheMessageIsSentAtOnceEvenWithoutATransactionsManager(): void
    {
        $this->db->unsetTransactionManager();

        $this->publisher(['deliver_immediately' => true])->publish(new OrderUpdated());

        self::assertCount(1, $this->transport->sent);
    }

    /**
     * @return array<string, array{string, string}>
     */
    public static function timezones(): array
    {
        return [
            'relay west of the publisher' => ['Europe/Berlin', 'America/New_York'],
            'relay east of the publisher' => ['America/New_York', 'Europe/Berlin'],
        ];
    }

    /**
     * A relay may run with another timezone than the publishing process (another container, `app.timezone` changed
     * by a deploy). Dates stored as local time without an offset were read as another moment: a message was held
     * for hours, or expired at once.
     */
    #[DataProvider('timezones')]
    public function testRelayInAnotherTimezoneSendsTheMessage(string $publisherTimezone, string $relayTimezone): void
    {
        $timezone = date_default_timezone_get();
        $now = CarbonImmutable::parse('2026-10-28 12:00:00', 'UTC');

        try {
            date_default_timezone_set($publisherTimezone);
            CarbonImmutable::setTestNow($now->setTimezone($publisherTimezone));
            $id = $this->publisher()->publish(new KeyedOrderUpdated(ttl: 60));

            date_default_timezone_set($relayTimezone);
            CarbonImmutable::setTestNow($now->addSeconds(10)->setTimezone($relayTimezone));
            $this->deliverer()->deliver(100);
        } finally {
            date_default_timezone_set($timezone);
        }

        self::assertSame([$id], array_map(fn($message) => $message->getMessageId(), $this->transport->sent));
        self::assertSame([], $this->rows());
    }

    public function testRowsOfAnotherTableNameAreUsed(): void
    {
        $this->db->getSchemaBuilder()->create('custom_outbox', function ($table) {
            foreach ($this->db->getSchemaBuilder()->getColumns('rabbitevents_outbox') as $column) {
                $table->addColumn($column['type_name'] === 'integer' ? 'integer' : 'text', $column['name'])->nullable();
            }
        });
        $store = new \RabbitEvents\Outbox\DatabaseOutboxStore($this->db, $this->codec, 'custom_outbox', false);

        $store->store(
            new \RabbitEvents\Outbox\PendingMessage(
                (new \RabbitEvents\Publisher\MessageFactory($this->registry))->make(new OrderUpdated()),
                null,
                null,
                \Carbon\CarbonImmutable::now(),
                $this->codec
            ),
            fn($pending, $new) => $new
        );

        self::assertSame(1, $this->db->table('custom_outbox')->count());
        self::assertSame([], $this->rows());
    }
}
