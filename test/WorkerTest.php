<?php

declare(strict_types=1);

namespace Resque;

use Resque\Failure\BackendInterface;
use Resque\Test\Job;

/**
 * Resque_Worker tests.
 *
 * @author         Chris Boulton <chris@bigcommerce.com>
 * @license        http://www.opensource.org/licenses/mit-license.php
 */
class WorkerTest extends Test
{
    private const QUEUE_JOBS                    = 'jobs';
    private const MESSAGE_CANNOT_RESERVE_JOB    = 'Cannot reserve job';
    private const MESSAGE_JOB_FROM_VALID_QUEUES = 'Job from valid queues';

    public function testWorkerRegistersInList(): void
    {
        $worker = $this->getWorker('*');

        // Make sure the worker is in the list
        $this->assertTrue((bool)$this->redis->sismember('resque:workers', (string)$worker));
    }

    public function testGetAllWorkers(): void
    {
        $workerCount = 3;

        // Register a few workers
        for ($i = 0; $i < $workerCount; ++$i) {
            $this->getWorker('queue_' . $i);
        }

        // Now try to get them
        $this->assertEquals($workerCount, count($this->resque->getWorkerIds()));
    }

    public function testGetWorkerById(): void
    {
        $worker = $this->getWorker('*');

        $newWorker = new Worker($this->resque, '*');
        $newWorker->setId((string)$worker);

        $this->assertEquals((string)$worker, (string)$newWorker);
    }

    public function testWorkerCanUnregister(): void
    {
        $worker = $this->getWorker('*');
        $worker->unregister();

        $this->assertFalse($this->resque->workerExists((string)$worker));
        $this->assertEquals([], $this->resque->getWorkerIds());
        $this->assertEquals([], $this->redis->smembers('resque:workers'));
    }

    public function testPausedWorkerDoesNotPickUpJobs(): void
    {
        $this->resque->clearQueue(self::QUEUE_JOBS);

        $worker = $this->getWorker('*');
        $worker->pauseProcessing(Worker::DEFAULT_SIGNO, null);

        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);

        $worker->work(0);
        $worker->work(0);

        $this->assertEquals(0, $worker->getStatistic('processed')->get());
    }

    public function testResumedWorkerPicksUpJobs(): void
    {
        $this->resque->clearQueue(self::QUEUE_JOBS);

        $worker = $this->getWorker('*');
        $worker->pauseProcessing(Worker::DEFAULT_SIGNO, 'pauseProcessing');

        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);
        $worker->work(0);

        $this->assertEquals(0, $worker->getStatistic('processed')->get());

        $worker->unPauseProcessing(Worker::DEFAULT_SIGNO, 'unPauseProcessing');
        $worker->work(0);

        $this->assertEquals(1, $worker->getStatistic('processed')->get());
    }

    public function testWorkerCanWorkOverMultipleQueues(): void
    {
        $queues = [
            'queue1',
            'queue2',
        ];

        $this->clearQueues($queues);

        $worker = $this->getWorker($queues);

        $this->resque->enqueue($queues[0], Job::class);
        $this->resque->enqueue($queues[1], Job::class);

        $job = $worker->reserve();

        if (!$job) {
            $this->fail(self::MESSAGE_CANNOT_RESERVE_JOB);
        }

        $this->assertContains($job->getQueue(), $queues, self::MESSAGE_JOB_FROM_VALID_QUEUES);

        $job = $worker->reserve();

        if (!$job) {
            $this->fail(self::MESSAGE_CANNOT_RESERVE_JOB);
        }

        $this->assertContains($job->getQueue(), $queues, self::MESSAGE_JOB_FROM_VALID_QUEUES);
    }

    public function testWildcardQueueWorkerWorksAllQueues(): void
    {
        $queues = [
            'queue1',
            'queue2',
        ];

        $this->clearQueues($queues);

        $worker = $this->getWorker('*');

        $this->resque->enqueue($queues[0], Job::class);
        $this->resque->enqueue($queues[1], Job::class);

        $job = $worker->reserve();

        if (!$job) {
            $this->fail(self::MESSAGE_CANNOT_RESERVE_JOB);
        }

        $this->assertContains($job->getQueue(), $queues, self::MESSAGE_JOB_FROM_VALID_QUEUES);

        $job = $worker->reserve();

        if (!$job) {
            $this->fail(self::MESSAGE_CANNOT_RESERVE_JOB);
        }

        $this->assertContains($job->getQueue(), $queues, self::MESSAGE_JOB_FROM_VALID_QUEUES);
    }

    public function testWorkerDoesNotWorkOnUnknownQueues(): void
    {
        $worker = $this->getWorker('queue1');

        $this->resque->enqueue('queue2', Job::class);

        $this->assertNull($worker->reserve());
    }

    public function testWorkerClearsItsStatusWhenNotWorking(): void
    {
        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);
        $worker = $this->getWorker(self::QUEUE_JOBS);
        $job    = $worker->reserve();

        if (!$job) {
            $this->fail('Could not reserve job');
        }

        $worker->workingOn($job);
        $worker->doneWorking();
        $this->assertEquals([], $worker->job());
    }

    public function testWorkerRecordsWhatItIsWorkingOn(): void
    {
        $worker = $this->getWorker(self::QUEUE_JOBS);

        $payload = [
            'class' => Job::class,
            'id'    => 'test',
        ];

        $job = new Job(self::QUEUE_JOBS, $payload);
        $worker->workingOn($job);

        $job = $worker->job();

        if (!is_array($job)) {
            $this->fail('Could not get job being worked on');
        }

        $this->assertEquals(self::QUEUE_JOBS, $job['queue']);

        if (!isset($job['run_at'])) {
            $this->fail('Job does not have run_at time');
        }

        $this->assertEquals($payload, $job['payload']);
    }

    public function testWorkerErasesItsStatsWhenShutdown(): void
    {
        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);
        $this->resque->enqueue(self::QUEUE_JOBS, 'Resque\Test\FailingJob');

        $worker = $this->getWorker(self::QUEUE_JOBS);

        $worker->work(0);
        $worker->shutdown(Worker::DEFAULT_SIGNO, 'shutdown');
        $worker->work(0);

        $this->resque->clearQueue(self::QUEUE_JOBS);

        $this->assertEquals(0, $worker->getStatistic('processed')->get());
        $this->assertEquals(0, $worker->getStatistic('failed')->get());
    }

    public function testWorkerCleansUpDeadWorkersOnStartup(): void
    {
        // Register a good worker
        $goodWorker = new Worker($this->resque, self::QUEUE_JOBS);
        $goodWorker->setLogger($this->logger);
        $goodWorker->register();
        $goodWorker = $this->getWorker(self::QUEUE_JOBS);

        $workerId = explode(':', (string)$goodWorker);

        // Register some bad workers
        $worker = new Worker($this->resque, self::QUEUE_JOBS);
        $worker->setLogger($this->logger);
        $worker->setId($workerId[0] . ':1:jobs');
        $worker->register();

        $worker = new Worker($this->resque, ['high', 'low']);
        $worker->setLogger($this->logger);
        $worker->setId($workerId[0] . ':2:high,low');
        $worker->register();

        $this->assertCount(3, $this->resque->getWorkerIds());

        $goodWorker->pruneDeadWorkers();

        // There should only be $goodWorker left now
        $this->assertCount(1, $this->resque->getWorkerIds());
    }

    public function testDeadWorkerCleanUpDoesNotCleanUnknownWorkers(): void
    {
        // Register a bad worker on this machine
        $worker = new Worker($this->resque, self::QUEUE_JOBS);
        $worker->setLogger($this->logger);
        $workerId = explode(':', (string)$worker);
        $worker->setId($workerId[0] . ':1:jobs');
        $worker->register();

        // Register some other false workers
        $worker = new Worker($this->resque, self::QUEUE_JOBS);
        $worker->setLogger($this->logger);
        $worker->setId('my.other.host:1:jobs');
        $worker->register();

        $this->assertCount(2, $this->resque->getWorkerIds());

        $worker->pruneDeadWorkers();

        // my.other.host should be left
        $workers = $this->resque->getWorkerIds();
        $this->assertCount(1, $workers);
        $this->assertEquals((string)$worker, (string)$workers[0]);
    }

    public function testWorkerFailsUncompletedJobsOnExit(): void
    {
        $backend = $this->getMockForAbstractClass(BackendInterface::class);

        $backend->expects($this->once())
            ->method('receiveFailure');

        $this->resque->setFailureBackend($backend);

        $worker = $this->getWorker(self::QUEUE_JOBS);

        $job = new Job(
            self::QUEUE_JOBS,
            [
                'class' => Job::class,
                'id'    => __METHOD__,
            ]
        );
        $job->setResque($this->resque);

        $worker->workingOn($job);
        $worker->unregister();
    }

    public function testBlockingListPop(): void
    {
        $worker = $this->getWorker(self::QUEUE_JOBS);

        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);
        $this->resque->enqueue(self::QUEUE_JOBS, Job::class);

        $i = 1;
        while ($job = $worker->reserve()) {
            $this->assertEquals(Job::class, $job['class']);

            if ($i == 2) {
                break;
            }

            $i++;
        }

        $this->assertEquals(2, $i);
    }

    public function testReestablishRedisConnection(): void
    {
        $client_mock = $this->getMockForAbstractClass('Resque\Client\ClientInterface');

        $resque = new Resque($client_mock);
        $worker = new Worker($resque, self::QUEUE_JOBS);

        $client_mock->expects($this->once())->method('isConnected')->willReturn(true);
        $client_mock->expects($this->once())->method('disconnect');
        $client_mock->expects($this->once())->method('connect');

        $worker->reestablishRedisConnection(Worker::DEFAULT_SIGNO, null);
    }

    protected function clearQueues(array $queues): void
    {
        foreach ($queues as $queue) {
            $this->resque->clearQueue($queue);
        }
    }
}
