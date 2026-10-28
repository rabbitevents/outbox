<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Outbox
    |--------------------------------------------------------------------------
    |
    | Published messages are written to the outbox table and delivered from
    | there. Published inside a database transaction, a message is written
    | in that transaction: it is sent only if the transaction commits.
    | `afterCommit` events reach the outbox after the commit.
    |
    */

    'enabled' => env('RABBITEVENTS_OUTBOX_ENABLED', true),

    // The connection of your business transaction: a message written on another connection
    // is not part of that transaction. null is the default connection.
    'connection' => env('RABBITEVENTS_OUTBOX_CONNECTION'),

    'table' => env('RABBITEVENTS_OUTBOX_TABLE', 'rabbitevents_outbox'),

    // Seconds a message may wait for delivery. An expired message is never sent: it stays in the
    // table with `expired_at`. An event can set its own with `outboxTtl()`. 0: never expires.
    'ttl' => (int) env('RABBITEVENTS_OUTBOX_TTL', 3600),

    // Events published directly, without the outbox (patterns as in `Str::is()`, e.g. 'metrics.*').
    // An event class can implement `RabbitEvents\Outbox\ShouldPublishDirectly` instead.
    'except' => [],

    // Send a message right after it is stored (after the commit). If that fails, the relay sends it.
    'deliver_immediately' => env('RABBITEVENTS_OUTBOX_DELIVER_IMMEDIATELY', true),

    // Seconds before the next try of a message that couldn't be sent; the last value repeats.
    'backoff' => [1, 5, 30, 60],

    // Lets several relays work on one table (MySQL 8+, MariaDB 10.6+, PostgreSQL 9.5+).
    // Set to false on older databases and run a single relay.
    'skip_locked' => env('RABBITEVENTS_OUTBOX_SKIP_LOCKED', true),
];
