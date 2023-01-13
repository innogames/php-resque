<?php

declare(strict_types=1);

namespace Resque\Test;

use Resque\JobInterface;

class MinimalJob implements JobInterface
{
    public bool $performed = false;

    public function __construct(string $queue, array $payload)
    {
        // No further setup needed for this minimal job
    }

    public function perform(): void
    {
        $this->performed = true;
    }

    public function getQueue(): string
    {
        return '';
    }

    public function getPayload(): array
    {
        return [];
    }
}
