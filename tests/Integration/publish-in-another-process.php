<?php

/**
 * Plays another process that publishes a keyed message: `php publish-in-another-process.php <table> <key> <payload json>`.
 * Prints the message ID once its transaction committed.
 */

declare(strict_types=1);

use RabbitEvents\Outbox\Tests\Integration\Environment;
use RabbitEvents\Outbox\Tests\RecordingTransport;
use RabbitEvents\Outbox\Tests\Unit\KeyedOrderUpdated;

require __DIR__ . '/../../vendor/autoload.php';

[, $table, $key, $payload] = $argv;

$connection = Environment::connection();
$publisher = Environment::publisher($connection, $table, new RecordingTransport());

echo $connection->transaction(fn() => $publisher->publish(new KeyedOrderUpdated(json_decode($payload, true), key: $key)));
