<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Console;

use Illuminate\Console\Command;
use RabbitEvents\Outbox\Contracts\OutboxStore;

class ForgetCommand extends Command
{
    protected $signature = 'rabbitevents:outbox:forget {id : The ID of the outbox message}';

    protected $description = 'Delete a message from the RabbitEvents outbox';

    public function handle(OutboxStore $store): int
    {
        $id = $this->argument('id');

        if (!is_string($id) || !$store->forget($id)) {
            $this->components->error('No outbox message with this ID.');

            return self::FAILURE;
        }

        $this->components->info('Outbox message deleted.');

        return self::SUCCESS;
    }
}
