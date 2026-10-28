<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Console;

use Carbon\CarbonImmutable;
use Illuminate\Console\Command;
use RabbitEvents\Outbox\Contracts\OutboxStore;

class PruneCommand extends Command
{
    protected $signature = 'rabbitevents:outbox:prune {--hours=168 : Delete messages that expired more than this many hours ago}';

    protected $description = 'Delete old expired messages from the RabbitEvents outbox';

    public function handle(OutboxStore $store): int
    {
        $deleted = $store->prune(CarbonImmutable::now()->subHours(max(0, (int) $this->option('hours'))));

        $this->components->info("Deleted {$deleted} expired messages.");

        return self::SUCCESS;
    }
}
