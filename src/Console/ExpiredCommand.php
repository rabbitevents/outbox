<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Console;

use Illuminate\Console\Command;
use RabbitEvents\Outbox\Contracts\OutboxStore;
use RabbitEvents\Outbox\PendingMessage;

class ExpiredCommand extends Command
{
    protected $signature = 'rabbitevents:outbox:expired {--limit=100 : Messages to show}';

    protected $description = 'List the outbox messages that expired before they could be sent';

    public function handle(OutboxStore $store): int
    {
        $messages = $store->expired(max(1, (int) $this->option('limit')));

        if ($messages === []) {
            $this->components->info('No expired messages.');

            return self::SUCCESS;
        }

        $this->table(
            ['ID', 'Message ID', 'Event', 'Key', 'Created', 'Expired', 'Attempts', 'Last error'],
            array_map(fn(PendingMessage $message) => [
                $message->id,
                $message->messageId(),
                $message->event(),
                $message->key,
                $message->createdAt->toDateTimeString(),
                $message->expiredAt?->toDateTimeString(),
                $message->attempts,
                $message->lastError,
            ], $messages)
        );

        return self::SUCCESS;
    }
}
