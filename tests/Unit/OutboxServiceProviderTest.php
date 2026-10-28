<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Unit;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\Capsule\Manager as Capsule;
use Mockery as m;
use RabbitEvents\Foundation\Context;
use RabbitEvents\Foundation\Serialization\JsonSerializer;
use RabbitEvents\Foundation\Serialization\SerializerRegistry;
use RabbitEvents\Outbox\Contracts\Merger;
use RabbitEvents\Outbox\DatabaseOutboxStore;
use RabbitEvents\Outbox\LatestWins;
use RabbitEvents\Outbox\Outbox;
use RabbitEvents\Outbox\OutboxServiceProvider;
use RabbitEvents\Publisher\Contracts\Outbox as OutboxContract;
use RabbitEvents\Publisher\PublisherServiceProvider;

class OutboxServiceProviderTest extends \PHPUnit\Framework\TestCase
{
    /**
     * @return array<string, array{0: list<class-string<\Illuminate\Support\ServiceProvider>>}>
     */
    public static function providerOrders(): array
    {
        return [
            'outbox first' => [[OutboxServiceProvider::class, PublisherServiceProvider::class]],
            'publisher first' => [[PublisherServiceProvider::class, OutboxServiceProvider::class]],
        ];
    }

    /**
     * @param list<class-string<\Illuminate\Support\ServiceProvider>> $providers
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('providerOrders')]
    public function testOutboxReplacesThePublishersNullOutbox(array $providers): void
    {
        $app = $this->application();

        foreach ($providers as $provider) {
            (new $provider($app))->register();
        }

        self::assertInstanceOf(Outbox::class, $app->make(OutboxContract::class));
        self::assertInstanceOf(LatestWins::class, $app->make(Merger::class));
        self::assertInstanceOf(DatabaseOutboxStore::class, $app->make(\RabbitEvents\Outbox\Contracts\OutboxStore::class));
        self::assertSame(3600, $app['config']->get('rabbitevents-outbox.ttl'));
    }

    public function testBoundMergerIsKept(): void
    {
        $app = $this->application();
        $merger = new class implements Merger {
            public function merge(\RabbitEvents\Outbox\PendingMessage $pending, \RabbitEvents\Outbox\PendingMessage $new): \RabbitEvents\Outbox\PendingMessage
            {
                return $pending;
            }
        };
        $app->instance(Merger::class, $merger);

        (new OutboxServiceProvider($app))->register(); // @phpstan-ignore argument.type (a container is enough to register)

        self::assertSame($merger, $app->make(Merger::class));
    }

    private function application(): Container
    {
        $app = new class extends Container {
            public function runningInConsole(): bool
            {
                return false;
            }
        };
        $app->instance('config', new Repository([
            'database' => ['default' => 'default', 'connections' => ['default' => ['driver' => 'sqlite', 'database' => ':memory:']]],
        ]));
        $capsule = new Capsule($app);
        $capsule->addConnection(['driver' => 'sqlite', 'database' => ':memory:']);
        $app->instance('db', $capsule->getDatabaseManager());
        $app->instance('events', new \Illuminate\Events\Dispatcher($app));
        $registry = new SerializerRegistry();
        $registry->register(new JsonSerializer());
        $app->instance(SerializerRegistry::class, $registry);
        $context = m::mock(Context::class);
        $context->shouldReceive('makeTopic')->andReturn(m::mock(\RabbitEvents\Foundation\Contracts\Destination::class));
        $context->shouldReceive('createProducer')->andReturn(m::mock(\RabbitEvents\Foundation\Contracts\Producer::class));
        $app->instance(Context::class, $context);

        return $app;
    }

    protected function tearDown(): void
    {
        m::close();
    }
}
