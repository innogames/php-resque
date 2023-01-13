<?php

declare(strict_types=1);

namespace Resque\Test;

use Exception;
use Resque\AbstractJob;

class FailingJob extends AbstractJob
{
    /**
     * Execute the job
     *
     * @throws Exception
     */
    public function perform(): void
    {
        throw new Exception('This job just failed');
    }
}
