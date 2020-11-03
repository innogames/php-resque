<?php

declare(strict_types=1);

namespace Resque;

/**
 * JobInterface
 *
 * Implement this to use a custom class hierarchy; that is, if you don't want
 * to subclass AbstractJob (which is probably much easier)
 */
interface JobInterface
{
    public function __construct(string $queue, array $payload);

    /**
     * Actually performs the work of the job
     */
    public function perform(): void;

    public function getQueue(): string;

    public function getPayload(): array;
}
