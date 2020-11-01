<?php

declare(strict_types=1);

namespace Resque\Job;

use Resque\Test;
use Resque\Test\FailingJob;
use Resque\Test\Job;

/**
 * Status tests.
 *
 * @package        Resque/Tests
 * @author         Chris Boulton <chris@bigcommerce.com>
 * @license        http://www.opensource.org/licenses/mit-license.php
 */
class StatusTest extends Test
{
    public function tearDown(): void
    {
        parent::tearDown();

        $this->resque->clearQueue('jobs');
    }

    public function testConstructor(): void
    {
        $status = new Status(uniqid(__CLASS__, true), $this->resque);

        $this->assertNotNull($status);
    }

    public function testJobStatusCanBeTracked(): void
    {
        $this->resque->clearQueue('jobs');
        $token = $this->resque->enqueue('jobs', Job::class, null, true);

        $status = new Status($token, $this->resque);
        $this->assertTrue($status->isTracking());
    }

    public function testJobStatusIsReturnedViaJobInstance(): void
    {
        $this->resque->clearQueue('jobs');

        $this->resque->enqueue('jobs', Job::class, null, true);

        $worker = $this->getWorker('jobs');
        $job    = $worker->reserve();

        if (!$job) {
            $this->fail('Could not get job');
        }

        $status = $this->resque->getStatusFactory()->forJob($job);
        $status = $status->get();

        $this->assertEquals(Status::STATUS_WAITING, $status);
    }

    public function testQueuedJobReturnsQueuedStatus(): void
    {
        $token  = $this->resque->enqueue('jobs', Job::class, null, true);
        $status = new Status($token, $this->resque);
        $this->assertEquals(Status::STATUS_WAITING, $status->get());
    }

    public function testQueuedJobReturnsCreatedAndUpdatedKeys(): void
    {
        $token  = $this->resque->enqueue('jobs', Job::class, null, true);
        $status = new Status($token, $this->resque);
        $this->assertGreaterThan(0, $status->getCreated());
        $this->assertGreaterThan(0, $status->getUpdated());
    }

    public function testStartingQueuedJobUpdatesUpdatedAtStatus(): void
    {
        $this->resque->clearQueue('jobs');

        $token = $this->resque->enqueue('jobs', Job::class, null, true);

        $status      = new Status($token, $this->resque);
        $old_created = $status->getCreated();
        $old_updated = $status->getUpdated();

        sleep(1);

        $worker = $this->getWorker('jobs');

        $job = $worker->reserve();

        if (!$job) {
            $this->fail('Cannot get job');
        }

        $worker->workingOn($job);

        $status = new Status($token, $this->resque);
        $this->assertEquals(Status::STATUS_RUNNING, $status->get());
        $this->assertEquals($old_created, $status->getCreated());
        $this->assertGreaterThan($old_updated, $status->getUpdated());
    }

    public function testRunningJobReturnsRunningStatus(): void
    {
        $this->resque->clearQueue('jobs');

        $token = $this->resque->enqueue('jobs', Job::class, null, true);

        $worker = $this->getWorker('jobs');

        $job = $worker->reserve();

        if (!$job) {
            $this->fail('Cannot get job');
        }

        $worker->workingOn($job);

        $status = new Status($token, $this->resque);
        $this->assertEquals(Status::STATUS_RUNNING, $status->get());
    }

    public function testFailedJobReturnsFailedStatus(): void
    {
        $this->resque->clearQueue('jobs');

        $token = $this->resque->enqueue('jobs', FailingJob::class, null, true);

        $worker = $this->getWorker('jobs');
        $worker->work(0);

        $status = new Status($token, $this->resque);
        $after  = $status->get();

        $this->assertEquals(Status::STATUS_FAILED, $after);
    }

    public function testCompletedJobIsRemovedAndStatusUnavailable(): void
    {
        $this->resque->clearQueue('jobs');

        $token  = $this->resque->enqueue('jobs', Job::class, null, true);
        $status = new Status($token, $this->resque);

        $this->assertEquals(Status::STATUS_WAITING, $status->get());

        $worker = $this->getWorker('jobs');
        $worker->work(0);

        $this->assertNull($status->get());
    }

    public function testStatusIsNotTrackedWhenToldNotTo(): void
    {
        $token  = $this->resque->enqueue('jobs', Job::class, null, false);
        $status = new Status($token, $this->resque);
        $this->assertFalse($status->isTracking());
    }

    public function testStatusTrackingCanBeStopped(): void
    {
        $status = new Status('test', $this->resque);
        $status->create();
        $this->assertEquals(Status::STATUS_WAITING, $status->get());

        $status->stop();
        $this->assertNull($status->get());
    }

    public function testRecreatedJobWithTrackingStillTracksStatus(): void
    {
        $worker        = $this->getWorker('jobs');
        $originalToken = $this->resque->enqueue('jobs', Job::class, null, true);

        /** @var Job $job */
        $job = $worker->reserve();

        if (!$job) {
            $this->fail('Could not reserve job');
        }

        // Mark this job as being worked on to ensure that the new status is still
        // waiting.
        $worker->workingOn($job);

        // Now recreate it
        $newToken = $job->recreate();

        // Make sure we've got a new job returned
        $this->assertNotEquals($originalToken, $newToken);

        // Now check the status of the new job
        /** @var Job $newJob */
        $newJob = $worker->reserve();

        if (!$newJob) {
            $this->fail('Could not get newJob');
        }

        $this->assertEquals(Status::STATUS_WAITING, $newJob->getStatus()->get());
    }
}
