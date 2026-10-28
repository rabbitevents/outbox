<?php

declare(strict_types=1);

namespace RabbitEvents\Outbox;

use Carbon\CarbonImmutable;
use Carbon\CarbonInterface;
use Closure;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use RabbitEvents\Outbox\Contracts\OutboxStore;

/**
 * Keeps the messages in a table of the connection of your business transaction: a message published in a
 * transaction is written in it.
 */
class DatabaseOutboxStore implements OutboxStore
{
    /**
     * Max length of `last_error`, so that a huge exception message can't break the update.
     */
    private const MAX_ERROR_LENGTH = 4096;

    public function __construct(
        private readonly Connection $db,
        private readonly MessageCodec $codec,
        private readonly string $table = 'rabbitevents_outbox',
        private readonly bool $skipLocked = true
    ) {
    }

    public function store(PendingMessage $message, Closure $merge): PendingMessage
    {
        if ($message->key === null) {
            return $message->stored($this->insert($message));
        }

        return $this->db->transaction(function () use ($message, $merge) {
            // Inserts the message, or takes an exclusive lock on the message of its key that waits for delivery
            // (after a relay that sends it: then it is gone, and this one is inserted). One statement, so two
            // publishers of one key don't deadlock, as an insert followed by a locking read would on MySQL.
            $this->query()->upsert([$this->insertColumns($message)], ['pending_key'], ['pending_key']);

            $row = $this->query()->where('pending_key', $message->key)->lockForUpdate()->first()
                ?? throw new \LogicException('The upserted outbox message is missing.');

            if ($row->message_id === $message->messageId()) {
                return $message->stored($row->id);
            }

            $merged = $merge($this->fromRow($row), $message);
            $this->query()->where('id', $row->id)->update($this->messageColumns($merged));

            return $merged->stored($row->id);
        });
    }

    public function afterCommit(Closure $callback): void
    {
        if ($this->db->transactionLevel() === 0) {
            $callback();

            return;
        }

        try {
            $this->db->afterCommit($callback);
        } catch (\RuntimeException) {
            // A connection without a transactions manager (outside a Laravel application) can't run it after the
            // commit, and before the commit the message may still be rolled back: the relay sends it.
        }
    }

    /**
     * Each message is taken in a transaction of its own: a relay that dies resends at most one message.
     */
    public function deliver(Closure $deliver, int $limit, ?array $ids = null): array
    {
        $candidates = $this->due()
            ->when($ids !== null, fn(Builder $query) => $query->whereIn('id', $ids))
            ->orderBy('id')
            ->limit($limit)
            ->get(['id', 'pending_key']);

        $results = [];

        foreach ($candidates as $candidate) {
            $result = $this->db->transaction(function () use ($candidate, $deliver) {
                if (!$row = $this->lock($candidate)) {
                    return null; // another relay has it, or it is no longer due
                }

                $message = $this->fromRow($row);

                if (!$delivery = $deliver($message)) {
                    return null;
                }

                $this->apply($row->id, $message, $delivery);

                return [$message, $delivery];
            });

            if ($result) {
                $results[] = $result;
            }
        }

        return $results;
    }

    /**
     * Lock a due message as a publisher locks it: one with a key through `pending_key`, before the row. Locking the
     * row first and `pending_key` on delete would deadlock with a publisher of that key on MySQL. With skip_locked
     * the relay never waits: a message another relay or a merging publisher holds is left for the next round.
     */
    private function lock(\stdClass $candidate): ?\stdClass
    {
        return $this->due()
            ->when(
                $candidate->pending_key !== null,
                fn(Builder $query) => $query->where('pending_key', $candidate->pending_key),
                fn(Builder $query) => $query->where('id', $candidate->id)
            )
            ->lock($this->skipLocked ? 'for update skip locked' : true)
            ->first();
    }

    private function due(): Builder
    {
        return $this->query()->whereNull('expired_at')->where('available_at', '<=', CarbonImmutable::now()->getTimestamp());
    }

    public function expired(int $limit): array
    {
        $rows = $this->query()->whereNotNull('expired_at')->orderBy('id')->limit($limit)->get()->all();

        return array_values(array_map(fn(\stdClass $row) => $this->fromRow($row), $rows));
    }

    public function forget(int|string $id): bool
    {
        return $this->query()->where('id', $id)->delete() > 0;
    }

    public function prune(CarbonInterface $expiredBefore): int
    {
        return $this->query()->where('expired_at', '<=', $expiredBefore->getTimestamp())->delete();
    }

    private function apply(int|string $id, PendingMessage $message, Delivery $delivery): void
    {
        match ($delivery->status) {
            DeliveryStatus::Sent => $this->query()->where('id', $id)->delete(),
            // Kept for the expired commands; the key is free for new messages of the object.
            DeliveryStatus::Expired => $this->query()->where('id', $id)->update([
                'expired_at' => CarbonImmutable::now()->getTimestamp(),
                'pending_key' => null,
            ]),
            DeliveryStatus::Failed => $this->query()->where('id', $id)->update([
                'attempts' => $message->attempts + 1,
                'last_error' => mb_strcut(
                    get_class($delivery->exception()) . ': ' . $delivery->exception()->getMessage(),
                    0,
                    self::MAX_ERROR_LENGTH
                ),
                'available_at' => $delivery->retryAt()->getTimestamp(),
            ]),
        };
    }

    private function insert(PendingMessage $message): int
    {
        return $this->query()->insertGetId($this->insertColumns($message));
    }

    /**
     * @return array<string, mixed>
     */
    private function insertColumns(PendingMessage $message): array
    {
        return $this->messageColumns($message) + ['attempts' => 0, 'available_at' => CarbonImmutable::now()->getTimestamp()];
    }

    /**
     * @return array<string, mixed>
     */
    private function messageColumns(PendingMessage $message): array
    {
        $encoded = $this->codec->encode($message->message);

        return [
            'message_id' => $encoded['message_id'],
            'event' => $encoded['event'],
            'body' => $encoded['body'],
            'properties' => $encoded['properties'],
            'key' => $message->key,
            'pending_key' => $message->key,
            'expires_at' => $message->expiresAt?->getTimestamp(),
            // The timestamp of the message, so that the relay sends it unchanged.
            'created_at' => $encoded['timestamp'] ?: $message->createdAt->getTimestamp(),
        ];
    }

    private function fromRow(\stdClass $row): PendingMessage
    {
        $createdAt = $this->date($row->created_at);

        return new PendingMessage(
            $this->codec->decode($row->message_id, $row->event, $row->body, $row->properties, $createdAt->getTimestamp()),
            $row->key,
            $row->expires_at === null ? null : $this->date($row->expires_at),
            $createdAt,
            $this->codec,
            $row->id,
            (int) $row->attempts,
            $row->last_error,
            $row->expired_at === null ? null : $this->date($row->expired_at)
        );
    }

    /**
     * In the timezone of this process, for display: comparisons use the stored timestamps.
     */
    private function date(int|string $timestamp): CarbonImmutable
    {
        return CarbonImmutable::createFromTimestamp((int) $timestamp, date_default_timezone_get());
    }

    private function query(): Builder
    {
        return $this->db->table($this->table);
    }
}
