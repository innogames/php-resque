<?php

declare(strict_types=1);

namespace Resque;

use ArrayAccess;
use ArrayIterator;
use InvalidArgumentException;
use IteratorAggregate;
use Resque\Exception\JobLogicException;
use Resque\Job\Status;
use Traversable;

/**
 * Resque job.
 *
 * @package        Resque/Job
 * @author         Chris Boulton <chris@bigcommerce.com>
 * @license        http://www.opensource.org/licenses/mit-license.php
 */
abstract class AbstractJob implements ArrayAccess, IteratorAggregate, JobInterface
{
    /**
     * @var string The ID of the job
     */
    protected $id;

    /**
     * @var string The name of the queue that this job belongs to.
     */
    protected $queue;

    /**
     * @var Resque The instance of Resque this job belongs to.
     */
    protected $resque;

    /**
     * @var array Containing details of the job.
     */
    protected $payload;

    /**
     * Instantiate a new instance of a job.
     *
     * @param string $queue   The queue that the job belongs to.
     * @param array  $payload array containing details of the job.
     */
    public function __construct(string $queue, array $payload)
    {
        $this->queue   = $queue;
        $this->payload = $payload;
        $this->id      = $payload['id'] ?? null;
    }

    public function setResque(Resque $resque): void
    {
        $this->resque = $resque;
    }

    /**
     * @return string
     */
    public function getQueue(): string
    {
        return $this->queue;
    }

    /**
     * Gets a status instance for this job
     *
     * @throws InvalidArgumentException
     * @throws JobLogicException
     */
    public function getStatus(): Status
    {
        if (!$this->resque) {
            throw new JobLogicException('Job has no Resque instance: cannot get status');
        }

        return $this->resque->getStatusFactory()->forJob($this);
    }

    public function getId(): string
    {
        return $this->id;
    }

    public function getPayload(): array
    {
        return $this->payload;
    }

    /**
     * Re-queue the current job.
     *
     * @return string ID of the recreated job
     */
    public function recreate(): string
    {
        $status   = $this->getStatus();
        $tracking = $status->isTracking();

        $new = $this->resque->enqueue(
            $this->queue,
            $this->payload['class'],
            $this->payload['args'],
            $tracking
        );

        if ($tracking) {
            $status->update(Status::STATUS_RECREATED);
            $status->setAttribute('recreated_as', $new);
        }

        return $new;
    }

    /**
     * Generate a string representation used to describe the current job.
     *
     * @return string The string representation of the job.
     */
    public function __toString()
    {
        $name = [
            'Job{' . $this->queue . '}',
        ];

        if (!empty($this->id)) {
            $name[] = 'ID: ' . $this->id;
        }

        $name[] = $this->payload['class'];

        if (!empty($this->payload['args'])) {
            $name[] = json_encode($this->payload['args']);
        }

        return '(' . implode(' | ', $name) . ')';
    }

    /**
     * @see ArrayAccess::offsetSet()
     */
    public function offsetSet(mixed $offset, mixed $value): void
    {
        if (is_null($offset)) {
            $this->payload[] = $value;
        } else {
            $this->payload[$offset] = $value;
        }
    }

    /**
     * @see ArrayAccess::offsetExists()
     */
    public function offsetExists(mixed $offset): bool
    {
        return isset($this->payload[$offset]);
    }

    /**
     * @see ArrayAccess::offsetUnset()
     */
    public function offsetUnset(mixed $offset): void
    {
        unset($this->payload[$offset]);
    }

    /**
     * @see ArrayAccess::offsetGet()
     */
    public function offsetGet(mixed $offset): mixed
    {
        return $this->payload[$offset] ?? null;
    }

    /**
     * @see IteratorAggregate::getIterator()
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->payload);
    }
}
