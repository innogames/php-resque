<?php

declare(strict_types=1);

namespace Resque;

use Resque\Test\FailingJob;
use Resque\Test\Job;
use Resque\Test\NoPerformJob;

/**
 * Job tests.
 *
 * @package        Resque/Tests
 * @author         Chris Boulton <chris@bigcommerce.com>
 * @license        http://www.opensource.org/licenses/mit-license.php
 */
class AbstractJobTest extends Test
{
    private const QUEUE_JOBS = 'jobs';

    /** @var Worker */
    protected $worker;

    public function setUp(): void
    {
        parent::setUp();

        // Register a worker to test with
        $this->worker = new Worker($this->resque, self::QUEUE_JOBS);
        $this->worker->setLogger($this->logger);
        $this->worker->register();
    }

    public function testJobCanBeQueued(): void
    {
        $this->assertNotEmpty($this->resque->enqueue(self::QUEUE_JOBS, Job::class));
    }

    public function testQueuedJobCanBeReserved(): void
    {
        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);

        $worker = $this->getWorker(self::QUEUE_JOBS);

        $job = $worker->reserve();

        if (!$job) {
            $this->fail('Job could not be reserved.');
        }

        $this->assertEquals(self::QUEUE_JOBS, $job->getQueue());
        $this->assertEquals(Job::class, $job['class']);
    }

    public function testFailedJobExceptionsAreCaught(): void
    {
        $this->resque->clearQueue(self::QUEUE_JOBS);

        $job = new FailingJob(
            self::QUEUE_JOBS,
            [
                'class' => FailingJob::class,
                'args'  => null,
                'id'    => 'failing_test_job',
            ]
        );
        $job->setResque($this->resque);

        $this->worker->perform($job);

        $failed       = new Statistic($this->resque, 'failed');
        $workerFailed = new Statistic($this->resque, 'failed:' . (string)$this->worker);

        $this->assertEquals(1, $failed->get());
        $this->assertEquals(1, $workerFailed->get());
    }

    public function testInvalidJobReservesNull(): void
    {
        $this->resque->enqueue(self::QUEUE_JOBS, NoPerformJob::class);
        $job = $this->worker->reserve();
        $this->assertNull($job);
    }
}
