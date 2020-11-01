<?php

declare(strict_types=1);

namespace Resque\Test;

use Resque\JobInterface;

class MinimalJob implements JobInterface
{
    public $performed = false;

    public function __construct(string $queue, array $payload)
    {
        //no further setup needed for this miniaml job
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
