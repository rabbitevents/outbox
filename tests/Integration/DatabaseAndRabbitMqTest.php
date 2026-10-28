<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Integration;

use Illuminate\Database\Connection;
use RabbitEvents\Foundation\Amqp\Connection as AmqpConnection;
use RabbitEvents\Foundation\Support\Sender;
use RabbitEvents\Outbox\Delivery;
use RabbitEvents\Outbox\PendingMessage;
use RabbitEvents\Outbox\Tests\RecordingTransport;
use RabbitEvents\Outbox\Tests\Unit\KeyedOrderUpdated;
use RabbitEvents\Outbox\Tests\Unit\OrderUpdated;

/**
 * Runs against the database (MySQL or PostgreSQL) and RabbitMQ of the integration docker-compose; skipped where
 * they are not reachable.
 */
class DatabaseAndRabbitMqTest extends \PHPUnit\Framework\TestCase
{
    private Connection $db;

    private string $table;

    protected function setUp(): void
    {
        try {
            $this->db = Environment::connection();
            $this->db->getPdo();
        } catch (\Throwable $e) {
            self::markTestSkipped(Environment::driver() . ' is not reachable at ' . Environment::databaseHost() . ': ' . $e->getMessage());
        }

        $this->table = uniqid('outbox_');
        Environment::migrate($this->db, $this->table);
    }

    protected function tearDown(): void
    {
        if (isset($this->table)) {
            $this->db->getSchemaBuilder()->dropIfExists($this->table);
        }
    }

    public function test_message_committed_with_the_transaction_reaches_rabbitmq_unchanged(): void
    {
        $event = uniqid('outbox.');
        $amqp = new AmqpConnection(['host' => Environment::rabbitmqHost(), 'vhost' => '/', 'exchange' => 'events']);
        $topic = $amqp->makeTopic();
        $consumer = $amqp->makeConsumer($amqp->makeQueue(uniqid('outbox-'), [$event], $topic));
        $publisher = Environment::publisher($this->db, $this->table, new Sender($topic, $amqp->createProducer()), [
            'deliver_immediately' => true,
        ]);

        $rolledBack = null;
        try {
            $this->db->transaction(function () use ($publisher, $event, &$rolledBack) {
                $rolledBack = $publisher->publish(new OrderUpdated(['id' => 1], $event));
                throw new \RuntimeException('Rolled back');
            });
        } catch (\RuntimeException) {
        }
        $id = $this->db->transaction(fn() => $publisher->publish(new OrderUpdated(['id' => 2, 'name' => 'Пётр'], $event)));

        $received = $consumer->receive(3000);
        $this->assertNotNull($received);
        $this->assertSame($id, $received->getMessageId());
        $this->assertSame(['id' => 2, 'name' => 'Пётр'], json_decode($received->getBody(), true));
        $this->assertSame($event, $received->getProperty('event'));
        $this->assertSame('application/json', $received->getProperty('content_type'));
        $this->assertNull($consumer->receive(500), 'The rolled back message must not be sent');
        $this->assertNotSame($rolledBack, $id);
        $this->assertSame(0, $this->db->table($this->table)->count());
    }

    public function test_relay_skips_the_messages_another_relay_holds_without_waiting(): void
    {
        $publisher = Environment::publisher($this->db, $this->table, new RecordingTransport());
        $ids = [];
        foreach ([1, 2, 3] as $order) {
            $ids[] = $publisher->publish(new OrderUpdated(['id' => $order]));
        }
        $first = new RecordingTransport();
        $second = new RecordingTransport();
        $secondRelay = Environment::deliverer(Environment::connection(), $this->table, $second);
        $waited = null;

        // While the first relay sends the first message (its transaction holds it), the second relay works.
        Environment::store($this->db, $this->table)->deliver(function (PendingMessage $message) use ($first, $secondRelay, &$waited) {
            $started = microtime(true);
            $secondRelay->deliver(100);
            $waited = microtime(true) - $started;
            $first->send($message->message);

            return Delivery::sent();
        }, 1);

        $this->assertSame([$ids[0]], array_map(fn($message) => $message->id(), $first->sent));
        $this->assertSame([$ids[1], $ids[2]], array_map(fn($message) => $message->id(), $second->sent));
        $this->assertLessThan(2.0, $waited, 'The second relay must not wait for the lock');
        $this->assertSame(0, $this->db->table($this->table)->count());
    }

    public function test_message_of_a_key_published_while_the_relay_sends_the_waiting_one(): void
    {
        $publisher = Environment::publisher($this->db, $this->table, new RecordingTransport());
        $first = $publisher->publish(new KeyedOrderUpdated(['status' => 'paid']));
        $transport = new RecordingTransport();
        $process = null;

        // The relay holds the waiting message while another process publishes a new one of the same key.
        Environment::store($this->db, $this->table)->deliver(function (PendingMessage $message) use ($transport, &$process) {
            $process = $this->publishInAnotherProcess('order:1', ['status' => 'shipped']);
            sleep(1); // the other process waits for this relay
            $transport->send($message->message);

            return Delivery::sent();
        }, 1);
        $second = $this->finish($process);

        $this->assertSame([$first], array_map(fn($message) => $message->id(), $transport->sent));
        $rows = $this->db->table($this->table)->get();
        $this->assertCount(1, $rows, 'The new message waits on its own');
        $this->assertSame($second, $rows[0]->message_id);
        $this->assertSame('order:1', $rows[0]->pending_key);
    }

    public function test_concurrent_transactions_merge_their_messages_of_one_key(): void
    {
        $publisher = Environment::publisher($this->db, $this->table, new RecordingTransport());
        $process = null;

        $this->db->transaction(function () use ($publisher, &$process) {
            $publisher->publish(new KeyedOrderUpdated(['status' => 'paid']));
            // The other transaction inserts the same key and waits for this one.
            $process = $this->publishInAnotherProcess('order:1', ['status' => 'shipped']);
            sleep(1);
        });
        $second = $this->finish($process);

        $rows = $this->db->table($this->table)->get();
        $this->assertCount(1, $rows, 'Merged into one message');
        $this->assertSame($second, $rows[0]->message_id, 'The latest wins');
        $this->assertSame(['status' => 'shipped'], json_decode((string) base64_decode($rows[0]->body), true));
    }

    /**
     * @param array<string, mixed> $payload
     * @return resource
     */
    private function publishInAnotherProcess(string $key, array $payload)
    {
        $process = proc_open(
            [PHP_BINARY, __DIR__ . '/publish-in-another-process.php', $this->table, $key, (string) json_encode($payload)],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes
        );
        $this->assertIsResource($process);
        $this->pipes = $pipes;

        return $process;
    }

    /** @var array<int, resource> */
    private array $pipes = [];

    /**
     * @param resource|null $process
     */
    private function finish($process): string
    {
        $this->assertIsResource($process);
        $output = stream_get_contents($this->pipes[1]);
        $errors = stream_get_contents($this->pipes[2]);
        $status = proc_close($process);

        $this->assertSame(0, $status, $output . $errors);

        return (string) $output;
    }
}
