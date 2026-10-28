<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        $this->schema()->create($this->table(), function (Blueprint $table) {
            $table->id();
            $table->uuid('message_id')->unique();
            $table->string('event');
            // Base64 of the message body: JSON or binary (Protobuf).
            $table->longText('body');
            $table->json('properties');
            // The key of the object the message is about; `pending_key` holds it only while the message waits for
            // delivery, so that one object has one waiting message.
            $table->string('key', 191)->nullable();
            $table->string('pending_key', 191)->nullable()->unique();
            $table->unsignedInteger('attempts')->default(0);
            $table->text('last_error')->nullable();
            // Unix timestamps: a date without an offset would be read as another moment by a process with another
            // timezone, and local time repeats an hour when the clocks go back.
            $table->unsignedBigInteger('available_at');
            $table->unsignedBigInteger('expires_at')->nullable();
            $table->unsignedBigInteger('expired_at')->nullable();
            $table->unsignedBigInteger('created_at');

            $table->index(['expired_at', 'available_at']);
        });
    }

    public function down(): void
    {
        $this->schema()->dropIfExists($this->table());
    }

    private function schema(): \Illuminate\Database\Schema\Builder
    {
        return Schema::connection(Config::get('rabbitevents-outbox.connection'));
    }

    private function table(): string
    {
        return Config::get('rabbitevents-outbox.table', 'rabbitevents_outbox');
    }
};
