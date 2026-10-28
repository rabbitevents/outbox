<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests;

use Carbon\CarbonImmutable;
use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Connection;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\Facades\Facade;
use Mockery;
use RabbitEvents\Foundation\Message;
use RabbitEvents\Foundation\Serialization\JsonSerializer;
use RabbitEvents\Foundation\Serialization\SerializerRegistry;
use RabbitEvents\Outbox\Contracts\Merger;
use RabbitEvents\Outbox\DatabaseOutboxStore;
use RabbitEvents\Outbox\Deliverer;
use RabbitEvents\Outbox\LatestWins;
use RabbitEvents\Outbox\MessageCodec;
use RabbitEvents\Outbox\Outbox;
use RabbitEvents\Publisher\MessageFactory;
use RabbitEvents\Publisher\Publisher;

abstract class TestCase extends \PHPUnit\Framework\TestCase
{
    protected Container $app;

    protected Connection $db;

    protected Dispatcher $events;

    protected SerializerRegistry $registry;

    protected MessageCodec $codec;

    protected DatabaseOutboxStore $store;

    protected RecordingTransport $transport;

    /**
     * What the publisher sends itself, without the outbox.
     */
    protected RecordingTransport $direct;

    /** @var list<\stdClass> */
    protected array $dispatched = [];

    protected function setUp(): void
    {
        parent::setUp();

        CarbonImmutable::setTestNow('2026-10-28 12:00:00');

        $this->app = new class extends Container {
            public function runningUnitTests(): bool
            {
                return true;
            }

            public function runningInConsole(): bool
            {
                return true;
            }
        };
        Container::setInstance($this->app);
        $this->app->bind(Merger::class, LatestWins::class);

        $capsule = new Capsule($this->app);
        $capsule->addConnection(['driver' => 'sqlite', 'database' => ':memory:']);
        $this->db = $capsule->getConnection();
        $this->db->setTransactionManager(new \Illuminate\Database\DatabaseTransactionsManager());
        $this->migrate($capsule);

        $this->events = new Dispatcher($this->app);
        $this->events->listen('*', fn(string $name, array $payload) => $this->dispatched[] = $payload[0]);

        $this->registry = new SerializerRegistry();
        $this->registry->register(new JsonSerializer());
        $this->codec = new MessageCodec($this->registry);
        $this->store = new DatabaseOutboxStore($this->db, $this->codec, skipLocked: false);
        $this->transport = new RecordingTransport();
    }

    protected function tearDown(): void
    {
        CarbonImmutable::setTestNow();
        Container::setInstance(null);
        Mockery::close();

        parent::tearDown();
    }

    /**
     * @param array<string, mixed> $config
     */
    protected function outbox(array $config = [], ?Deliverer $deliverer = null): Outbox
    {
        return new Outbox(
            $this->store,
            $deliverer ?? $this->deliverer(),
            $this->codec,
            $this->app,
            $this->events,
            $config + ['deliver_immediately' => false]
        );
    }

    protected function deliverer(): Deliverer
    {
        return new Deliverer($this->store, $this->transport, $this->events, [1, 5, 30]);
    }

    /**
     * The publisher of the core with the outbox; `$this->direct` receives what it sends itself.
     *
     * @param array<string, mixed> $config
     */
    protected function publisher(array $config = []): Publisher
    {
        $this->direct = new RecordingTransport();

        return new Publisher(new MessageFactory($this->registry), $this->direct, $this->outbox($config), $this->events);
    }

    /**
     * @return list<\stdClass>
     */
    protected function rows(): array
    {
        return array_values($this->db->table('rabbitevents_outbox')->orderBy('id')->get()->all());
    }

    /**
     * A stored timestamp as a date in the timezone of the tests.
     */
    protected function date(int|string|null $timestamp): ?string
    {
        return $timestamp === null ? null : CarbonImmutable::createFromTimestamp((int) $timestamp)->format('Y-m-d H:i:s');
    }

    /**
     * @template T
     * @param class-string<T> $class
     * @return list<T>
     */
    protected function dispatched(string $class): array
    {
        return array_values(array_filter($this->dispatched, fn($event) => $event instanceof $class));
    }

    private function migrate(Capsule $capsule): void
    {
        // The migration uses the Schema and Config facades, as in an application.
        $this->app->instance('db', $capsule->getDatabaseManager());
        $this->app->instance('config', new Repository([
            'database' => ['default' => 'default', 'connections' => ['default' => ['driver' => 'sqlite', 'database' => ':memory:']]],
            'rabbitevents-outbox' => ['table' => 'rabbitevents_outbox'],
        ]));
        Facade::setFacadeApplication($this->app); // @phpstan-ignore argument.type (a container is enough for these facades)

        (require __DIR__ . '/../database/migrations/create_rabbitevents_outbox_table.php')->up();

        // The publisher reads Laravel Context through a facade when an application is set.
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
    }
}

class RecordingTransport implements \RabbitEvents\Foundation\Contracts\Transport
{
    /** @var list<Message> */
    public array $sent = [];

    public ?\Throwable $failWith = null;

    public function send(Message $message): void
    {
        if ($this->failWith) {
            throw $this->failWith;
        }

        $this->sent[] = $message;
    }
}
