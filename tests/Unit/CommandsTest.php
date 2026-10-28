<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox\Tests\Unit;

use Carbon\CarbonImmutable;
use Illuminate\Console\Command;
use Illuminate\Console\OutputStyle;
use RabbitEvents\Outbox\Console\ExpiredCommand;
use RabbitEvents\Outbox\Console\ForgetCommand;
use RabbitEvents\Outbox\Console\PruneCommand;
use RabbitEvents\Outbox\Console\RelayCommand;
use RabbitEvents\Outbox\Contracts\OutboxStore;
use RabbitEvents\Outbox\Deliverer;
use RabbitEvents\Outbox\Tests\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;

class CommandsTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->app->instance(OutboxStore::class, $this->store);
        $this->app->instance(Deliverer::class, $this->deliverer());
    }

    public function testRelaySendsWaitingMessages(): void
    {
        $this->publisher()->publish(new OrderUpdated());

        [$status] = $this->runCommand(new RelayCommand(), ['--once' => true]);

        self::assertSame(Command::SUCCESS, $status);
        self::assertCount(1, $this->transport->sent);
    }

    public function testRelayStopsWhenAMessageCannotBeSent(): void
    {
        // Its connection to the broker may be broken for good: a new relay gets a new one.
        $this->publisher()->publish(new OrderUpdated());
        $this->transport->failWith = new \RuntimeException('Broker unavailable');

        [$status, $output] = $this->runCommand(new RelayCommand());

        self::assertSame(Command::FAILURE, $status);
        self::assertStringContainsString('could not be sent', $output);
        self::assertSame(1, (int) $this->rows()[0]->attempts);
    }

    public function testRelayStopsOnSignal(): void
    {
        $relay = new class extends RelayCommand {
            public int $sleeps = 0;

            protected function sleep(float $seconds): void
            {
                $this->sleeps++;
                posix_kill((int) getmypid(), SIGTERM);
                pcntl_signal_dispatch();
            }
        };

        [$status] = $this->runCommand($relay);

        self::assertSame(Command::SUCCESS, $status);
        self::assertSame(1, $relay->sleeps);
    }

    public function testExpiredListsExpiredMessages(): void
    {
        $id = $this->publisher()->publish(new KeyedOrderUpdated(ttl: 60));
        CarbonImmutable::setTestNow('2026-10-28 13:00:00');
        $this->deliverer()->deliver(100);

        [$status, $output] = $this->runCommand(new ExpiredCommand());

        self::assertSame(Command::SUCCESS, $status);
        self::assertStringContainsString((string) $id, $output);
        self::assertStringContainsString('order:1', $output);
        self::assertStringContainsString('2026-10-28 13:00:00', $output);
    }

    public function testExpiredWithoutExpiredMessages(): void
    {
        [, $output] = $this->runCommand(new ExpiredCommand());

        self::assertStringContainsString('No expired messages', $output);
    }

    public function testForgetDeletesAMessage(): void
    {
        $this->publisher()->publish(new OrderUpdated());

        [$status] = $this->runCommand(new ForgetCommand(), ['id' => (string) $this->rows()[0]->id]);
        [$missing] = $this->runCommand(new ForgetCommand(), ['id' => '999']);

        self::assertSame(Command::SUCCESS, $status);
        self::assertSame([], $this->rows());
        self::assertSame(Command::FAILURE, $missing);
    }

    public function testPruneDeletesOldExpiredMessagesOnly(): void
    {
        $publisher = $this->publisher();
        $publisher->publish(new KeyedOrderUpdated(['id' => 1], ttl: 60, key: 'order:1'));
        CarbonImmutable::setTestNow('2026-10-20 12:00:00');
        $publisher->publish(new KeyedOrderUpdated(['id' => 2], ttl: 60, key: 'order:2'));
        CarbonImmutable::setTestNow('2026-10-20 12:05:00');
        $this->deliverer()->deliver(100); // order:2 expires on Oct 20
        CarbonImmutable::setTestNow('2026-10-28 12:05:00');
        $this->deliverer()->deliver(100); // order:1 expires on Oct 28
        $publisher->publish(new OrderUpdated(['id' => 3])); // waiting

        [$status, $output] = $this->runCommand(new PruneCommand(), ['--hours' => '24']);

        self::assertSame(Command::SUCCESS, $status);
        self::assertStringContainsString('Deleted 1', $output);
        self::assertSame(['order:1', null], array_map(
            fn($row) => json_decode($row->body ? base64_decode($row->body) : 'null', true)['id'] === 1 ? 'order:1' : null,
            $this->rows()
        ));
    }

    /**
     * Dates are stored to the second: a message that expired in the current second is expired "0 hours ago" too.
     */
    public function testPruneWithZeroHoursDeletesMessagesExpiredInTheCurrentSecond(): void
    {
        $this->publisher()->publish(new KeyedOrderUpdated(ttl: 60));
        CarbonImmutable::setTestNow('2026-10-28 13:00:00.400');
        $this->deliverer()->deliver(100);
        CarbonImmutable::setTestNow('2026-10-28 13:00:00.800');

        [$status, $output] = $this->runCommand(new PruneCommand(), ['--hours' => '0']);

        self::assertSame(Command::SUCCESS, $status);
        self::assertStringContainsString('Deleted 1', $output);
        self::assertSame([], $this->rows());
    }

    /**
     * @param array<string, mixed> $input
     * @return array{0: int, 1: string}
     */
    private function runCommand(Command $command, array $input = []): array
    {
        $command->setLaravel($this->app);
        $output = new BufferedOutput();
        $this->app->instance(OutputStyle::class, new OutputStyle(new ArrayInput([]), $output));

        $status = $command->run(new ArrayInput($input), $output);

        return [$status, $output->fetch()];
    }
}
