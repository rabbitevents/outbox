<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Integration;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\Connection;
use Illuminate\Database\DatabaseTransactionsManager;
use Illuminate\Events\Dispatcher;
use Illuminate\Support\Facades\Facade;
use RabbitEvents\Foundation\Contracts\Transport;
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

/**
 * The real services of the integration docker-compose: a database (MySQL `mysql` by default; OUTBOX_DB_DRIVER=pgsql
 * and OUTBOX_DB_HOST for PostgreSQL) and RabbitMQ (`rabbitmq`).
 * Shared by the tests and by the script that plays a concurrent process.
 */
final class Environment
{
    public static function driver(): string
    {
        return getenv('OUTBOX_DB_DRIVER') ?: 'mysql';
    }

    public static function databaseHost(): string
    {
        return getenv('OUTBOX_DB_HOST') ?: (self::driver() === 'pgsql' ? 'postgres' : 'mysql');
    }

    public static function rabbitmqHost(): string
    {
        return getenv('RABBITEVENTS_HOST') ?: 'rabbitmq';
    }

    /**
     * A new connection (a new database session) with a transactions manager, as in a Laravel application.
     */
    public static function connection(): Connection
    {
        $capsule = new Capsule(new Container());
        $capsule->addConnection(self::driver() === 'pgsql' ? [
            'driver' => 'pgsql',
            'host' => self::databaseHost(),
            'database' => 'outbox',
            'username' => 'postgres',
            'password' => 'secret',
        ] : [
            'driver' => 'mysql',
            'host' => self::databaseHost(),
            'database' => 'outbox',
            'username' => 'root',
            'password' => 'secret',
        ]);

        $connection = $capsule->getConnection();
        $connection->setTransactionManager(new DatabaseTransactionsManager());

        return $connection;
    }

    public static function migrate(Connection $connection, string $table): void
    {
        $app = new Container();
        $app->instance('db', new class ($connection) {
            public function __construct(private readonly Connection $connection)
            {
            }

            public function connection(?string $name = null): Connection
            {
                return $this->connection;
            }
        });
        $app->instance('config', new Repository(['rabbitevents-outbox' => ['table' => $table]]));
        Facade::setFacadeApplication($app); // @phpstan-ignore argument.type (a container is enough for these facades)

        try {
            (require __DIR__ . '/../../database/migrations/create_rabbitevents_outbox_table.php')->up();
        } finally {
            Facade::clearResolvedInstances();
            Facade::setFacadeApplication(null);
        }
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function publisher(Connection $connection, string $table, Transport $transport, array $config = []): Publisher
    {
        $app = new Container();
        $app->bind(Merger::class, LatestWins::class);
        $events = new Dispatcher($app);
        $registry = new SerializerRegistry();
        $registry->register(new JsonSerializer());
        $codec = new MessageCodec($registry);
        $store = new DatabaseOutboxStore($connection, $codec, $table);

        return new Publisher(
            new MessageFactory($registry),
            $transport,
            new Outbox($store, new Deliverer($store, $transport, $events), $codec, $app, $events, $config + ['deliver_immediately' => false]),
            $events
        );
    }

    public static function deliverer(Connection $connection, string $table, Transport $transport): Deliverer
    {
        return new Deliverer(self::store($connection, $table), $transport, new Dispatcher(new Container()));
    }

    public static function store(Connection $connection, string $table): DatabaseOutboxStore
    {
        $registry = new SerializerRegistry();
        $registry->register(new JsonSerializer());

        return new DatabaseOutboxStore($connection, new MessageCodec($registry), $table);
    }
}
