<?php

declare(strict_types=1);

namespace Resque\Job;

use Resque\Exception\JobIdException;
use Resque\JobInterface;
use Resque\Resque;

class StatusFactory
{
    /**
     * @var Resque
     */
    protected $resque;

    public function __construct(Resque $resque)
    {
        $this->resque = $resque;
    }

    public function forId(string $id): Status
    {
        return new Status($id, $this->resque);
    }

    /**
     * @throws JobIdException
     */
    public function forJob(JobInterface $job): Status
    {
        $payload = $job->getPayload();

        if (empty($payload['id'])) {
            throw new JobIdException('Job has no ID in payload, cannot get Status object');
        }

        return $this->forId($payload['id']);
    }
}
