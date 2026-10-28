<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Console;

use Illuminate\Console\Command;
use RabbitEvents\Outbox\Deliverer;

class RelayCommand extends Command
{
    protected $signature = 'rabbitevents:outbox:relay
                            {--sleep=1 : Seconds to wait when no message is due}
                            {--batch=100 : Messages taken at once}
                            {--memory=128 : The memory limit in megabytes}
                            {--once : Deliver one batch and exit}';

    protected $description = 'Send the messages waiting in the RabbitEvents outbox';

    private bool $shouldQuit = false;

    public function handle(Deliverer $deliverer): int
    {
        $this->listenForSignals();

        $batch = max(1, (int) $this->option('batch'));

        do {
            $report = $deliverer->deliver($batch);

            if ($report->failed()) {
                // The connection to the broker may be broken for good: let the process manager start a new relay.
                // The messages wait for their next try.
                $this->components->error('A message could not be sent; the relay stops.');

                return self::FAILURE;
            }

            if ($this->option('once') || $this->memoryExceeded()) {
                return self::SUCCESS;
            }

            if ($report->total() < $batch) {
                $this->sleep((float) $this->option('sleep'));
            }
        } while (!$this->shouldQuit);

        return self::SUCCESS;
    }

    protected function sleep(float $seconds): void
    {
        // In steps, so that SIGTERM stops a relay that has nothing to do at once.
        for ($slept = 0.0; $slept < $seconds && !$this->shouldQuit; $slept += 0.1) {
            usleep(100_000);
        }
    }

    private function memoryExceeded(): bool
    {
        return memory_get_usage(true) / 1024 / 1024 >= (int) $this->option('memory');
    }

    private function listenForSignals(): void
    {
        if (!extension_loaded('pcntl')) {
            return;
        }

        pcntl_async_signals(true);

        foreach ([SIGTERM, SIGINT, SIGQUIT] as $signal) {
            pcntl_signal($signal, fn() => $this->shouldQuit = true);
        }
    }
}
