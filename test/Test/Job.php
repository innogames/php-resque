<?php

declare(strict_types=1);

namespace Resque\Test;

use Resque\AbstractJob;
use Resque\Job\Status;

class Job extends AbstractJob
{
    public $performed = false;

    public function perform(): void
    {
        $this->performed = true;
    }

    /**
     * @see Status
     */
    public function getStatusCode(): ?int
    {
        return $this->getStatus()->get();
    }
}
