<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Illuminate\Support\ServiceProvider;
use RabbitEvents\Foundation\Context;
use RabbitEvents\Foundation\Serialization\SerializerRegistry;
use RabbitEvents\Foundation\Support\Sender;
use RabbitEvents\Outbox\Console\ExpiredCommand;
use RabbitEvents\Outbox\Console\ForgetCommand;
use RabbitEvents\Outbox\Console\PruneCommand;
use RabbitEvents\Outbox\Console\RelayCommand;
use RabbitEvents\Outbox\Contracts\Merger;
use RabbitEvents\Outbox\Contracts\OutboxStore;
use RabbitEvents\Publisher\Contracts\Outbox as OutboxContract;

class OutboxServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/rabbitevents-outbox.php', 'rabbitevents-outbox');

        $this->app->singleton(MessageCodec::class, static fn($app) => new MessageCodec($app[SerializerRegistry::class]));

        $this->app->bindIf(Merger::class, LatestWins::class);

        $this->app->singletonIf(OutboxStore::class, static fn($app) => new DatabaseOutboxStore(
            $app['db']->connection($app['config']->get('rabbitevents-outbox.connection')),
            $app[MessageCodec::class],
            (string) $app['config']->get('rabbitevents-outbox.table', 'rabbitevents_outbox'),
            (bool) $app['config']->get('rabbitevents-outbox.skip_locked', true)
        ));

        $this->app->singleton(Deliverer::class, static fn($app) => new Deliverer(
            $app[OutboxStore::class],
            new Sender($app[Context::class]->makeTopic(), $app[Context::class]->createProducer()),
            $app['events'],
            array_values(array_map('intval', (array) $app['config']->get('rabbitevents-outbox.backoff', [1, 5, 30, 60])))
        ));

        // Replaces the publisher's NullOutbox, whichever provider registers first.
        $this->app->singleton(OutboxContract::class, static fn($app) => new Outbox(
            $app[OutboxStore::class],
            $app[Deliverer::class],
            $app[MessageCodec::class],
            $app,
            $app['events'],
            (array) $app['config']->get('rabbitevents-outbox')
        ));
    }

    public function boot(): void
    {
        if (!$this->app->runningInConsole()) {
            return;
        }

        $this->publishes([
            __DIR__ . '/../config/rabbitevents-outbox.php' => $this->app->configPath('rabbitevents-outbox.php'),
        ], 'rabbitevents-outbox-config');

        $this->publishes([
            __DIR__ . '/../database/migrations/create_rabbitevents_outbox_table.php' => $this->app->databasePath(
                'migrations/' . date('Y_m_d_His') . '_create_rabbitevents_outbox_table.php'
            ),
        ], 'rabbitevents-outbox-migrations');

        $this->commands([RelayCommand::class, ExpiredCommand::class, ForgetCommand::class, PruneCommand::class]);
    }
}
