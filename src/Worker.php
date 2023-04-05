<?php

declare(strict_types=1);

namespace Resque;

use Exception;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Resque\Exception\DirtyExitException;
use Resque\Exception\JobClassNotFoundException;
use Resque\Exception\JobIdException;
use Resque\Exception\JobInvalidException;
use Resque\Job\Status;
use RuntimeException;

use function array_merge;
use function class_exists;
use function function_exists;
use function gethostname;
use function getmypid;
use function implode;
use function is_numeric;
use function is_subclass_of;
use function method_exists;
use function pcntl_fork;
use function pcntl_signal;
use function php_uname;
use function preg_match;
use function shuffle;
use function sort;
use function sprintf;

use const SIGCONT;
use const SIGINT;
use const SIGPIPE;
use const SIGQUIT;
use const SIGTERM;
use const SIGUSR1;
use const SIGUSR2;

/**
 * Resque worker that handles checking queues for jobs, fetching them
 * off the queues, running them and handling the result.
 *
 * @author  Chris Boulton <chris@bigcommerce.com>
 * @license http://www.opensource.org/licenses/mit-license.php
 */
class Worker implements LoggerAwareInterface
{
    /**
     * This is not actually evaluated, but as the signal handlers require the signal number to be supplied, we just
     * assume 15 (SIGTERM) by default
     */
    public const DEFAULT_SIGNO = 15;

    /**
     * @var string String identifying this worker.
     */
    protected $id;

    /**
     * @var LoggerInterface Logging object that implements the PSR-3 LoggerInterface
     */
    protected $logger;

    /**
     * @var array Array of all associated queues for this worker.
     */
    protected $queues = [];

    /**
     * Whether the worker should refresh the list of queues on reserve, or just go with the queues it has been given
     *
     * Passing * as a queue name will cause the worker to listen on all queues, and also refresh them (so that you
     * don't need to restart workers when you add a queue)
     *
     * @var bool
     */
    protected $refreshQueues = false;

    /**
     * @var bool True if on the next iteration, the worker should shutdown.
     */
    protected $shutdown = false;

    /**
     * @var bool True if this worker is paused.
     */
    protected $paused = false;

    /**
     * @var JobInterface|null Current job, if any, being processed by this worker.
     */
    protected $currentJob = null;

    /**
     * @var mixed[]
     */
    protected $options = [];

    /**
     * @var int Process ID of child worker processes.
     */
    private $child = null;

    /**
     * @var Resque
     */
    protected $resque;

    /**
     * Instantiate a new worker, given a list of queues that it should be working
     * on. The list of queues should be supplied in the priority that they should
     * be checked for jobs (first come, first served)
     *
     * Passing a single '*' allows the worker to work on all queues.
     * You can easily add new queues dynamically and have them worked on using this method.
     *
     * @param Resque       $resque
     * @param string|array $queues String with a single queue name, array with multiple.
     * @param array        $options
     */
    public function __construct(Resque $resque, $queues, array $options = [])
    {
        $this->configure($options);

        $this->queues = is_array($queues) ? $queues : [$queues];
        $this->resque = $resque;
        $this->logger = $this->resque->getLogger();

        if (in_array('*', $this->queues) || empty($this->queues)) {
            $this->refreshQueues = true;
            $this->queues        = $resque->queues();
        }

        $this->configureId();
    }

    /**
     * Set the ID of this worker to a given ID string.
     */
    public function setId(string $id)
    {
        $this->id = $id;
    }

    /**
     * Gives access to the main Resque system this worker belongs to
     */
    public function getResque(): Resque
    {
        return $this->resque;
    }

    /**
     * Given a worker ID, check if it is registered/valid.
     */
    public function exists(string $id): bool
    {
        return (bool)$this->resque->getClient()->sismember('workers', $id);
    }

    /**
     * The primary loop for a worker which when called on an instance starts
     * the worker's life cycle.
     *
     * Queues are checked every $interval (seconds) for new jobs.
     *
     * @param int $interval How often to check for new jobs across the queues.
     */
    public function work(int $interval = 5)
    {
        $this->updateProcLine('Starting');
        $this->startup();

        while (true) {
            pcntl_signal_dispatch();
            if ($this->shutdown) {
                $this->unregister();

                return;
            }

            // Attempt to find and reserve a job
            $job = false;
            if (!$this->paused) {
                $job = $this->reserve();
            }

            if (!$job) {
                // For an interval of 0, continue now - helps with unit testing etc
                if ($interval == 0) {
                    break;
                }

                // If no job was found, we sleep for $interval before continuing and checking again
                if ($this->paused) {
                    $this->updateProcLine('Paused');
                } else {
                    $this->updateProcLine('Waiting for ' . implode(',', $this->queues));
                }

                usleep($interval * 1000000);
                continue;
            }

            $this->logger->info('got {job}', ['job' => $job]);
            $this->workingOn($job);

            if ($this->options['no_fork']) {
                $this->child = null;
            } else {
                $this->child = $this->fork();
            }

            if (!$this->child) {
                // Forked and we're the child. Run the job.
                $status = 'Processing ' . $job->getQueue() . ' since ' . date("Y-m-d H:i:s", time());;
                $this->updateProcLine($status);
                $this->logger->notice($status);
                $this->perform($job);

                if (function_exists('pcntl_fork') && !$this->options['no_fork']) {
                    exit(0);
                }
            } elseif ($this->child > 0) {
                // Parent process, sit and wait
                $status = 'Forked ' . $this->child . ' at ' . date("Y-m-d H:i:s", time());;
                $this->updateProcLine($status);
                $this->logger->info($status);

                // Wait until the child process finishes before continuing
                pcntl_wait($status);
                $exitStatus = pcntl_wexitstatus($status);

                if ($exitStatus !== 0) {
                    $this->failJob(
                        $job,
                        new DirtyExitException(
                            'Job exited with exit code ' . $exitStatus
                        )
                    );
                } else {
                    $this->logger->debug('Job returned status code {code}', ['code' => $exitStatus]);
                }
            }

            $this->doneWorking();
        }
    }

    /**
     * Process a single job.
     */
    public function perform(JobInterface $job)
    {
        try {
            if ($this->options['pre_perform']) {
                $this->options['pre_perform']();
            }

            $this->performJob($job);

            if ($this->options['post_perform']) {
                $this->options['post_perform']();
            }
        } catch (Exception $e) {
            $this->logger->notice(
                '{job} failed: {exception}',
                [
                    'job'       => $job,
                    'exception' => $e,
                ]
            );
            $this->failJob($job, $e);

            return;
        }

        try {
            $this->resque->getStatusFactory()->forJob($job)->update(Status::STATUS_COMPLETE);
        } catch (JobIdException $e) {
            $this->logger->warning('Could not mark job complete: no ID in payload - {exception}', ['exception' => $e]);
        }

        $payload = $job->getPayload();

        $this->logger->notice(
            'Finished job {queue}/{class} (ID: {id})',
            [
                'queue' => $job->getQueue(),
                'class' => get_class($job),
                'id'    => isset($payload['id']) ? $payload['id'] : 'unknown',
            ]
        );

        $this->logger->debug('Done with {job}', ['job' => $job]);
    }

    /**
     * Attempt to find a job from the top of one of the queues for this worker.
     *
     * @return JobInterface|null Instance of JobInterface if a job is found, null if not.
     */
    public function reserve(): ?JobInterface
    {
        $this->refreshQueues();

        $this->logger->debug(
            'Attempting to reserve job from {queues}',
            [
                'queues' => empty($this->queues) ? 'empty queue list' : implode(', ', $this->queues),
            ]
        );

        foreach ($this->queues as $queue) {
            $payload = $this->resque->pop($queue);

            if (!is_array($payload)) {
                continue;
            }

            $payload['queue'] = $queue;

            try {
                $job = $this->createJobInstance($queue, $payload);
            } catch (ResqueException $exception) {
                $this->failJob($payload, $exception);

                return null;
            }

            if ($job) {
                $this->logger->info('Found job on {queue}', ['queue' => $queue]);

                return $job;
            }
        }

        return null;
    }

    /**
     * Signal handler callback for USR2, pauses processing of new jobs.
     *
     * @param int   $signo
     * @param mixed $signinfo
     */
    public function pauseProcessing($signo, $signinfo)
    {
        $this->logger->notice('USR2 received; pausing job processing');
        $this->paused = true;
    }

    /**
     * Signal handler callback for CONT, resumes worker allowing it to pick
     * up new jobs.
     *
     * @param int   $signo
     * @param mixed $signinfo
     */
    public function unPauseProcessing($signo, $signinfo)
    {
        $this->logger->notice('CONT received; resuming job processing');
        $this->paused = false;
    }

    /**
     * Signal handler for SIGPIPE, in the event the redis connection has gone away.
     * Attempts to reconnect to redis, or raises an Exception.
     *
     * @param int   $signo
     * @param mixed $signinfo
     */
    public function reestablishRedisConnection($signo, $signinfo)
    {
        $this->logger->notice('SIGPIPE received; attempting to reconnect');
        $this->resque->reconnect();
    }

    /**
     * Schedule a worker for shutdown. Will finish processing the current job
     * and when the timeout interval is reached, the worker will shut down.
     *
     * @param int   $signo
     * @param mixed $signinfo
     */
    public function shutdown($signo, $signinfo)
    {
        $this->shutdown = true;
        $this->logger->notice('Exiting...');
    }

    /**
     * Force an immediate shutdown of the worker, killing any child jobs
     * currently running.
     *
     * @param int   $signo
     * @param mixed $signinfo
     */
    public function shutdownNow($signo, $signinfo)
    {
        $this->shutdown($signo, $signinfo);
        $this->killChild($signo, $signinfo);
    }

    /**
     * Kill a forked child job immediately. The job it is processing will not
     * be completed.
     *
     * @param int   $signo
     * @param mixed $signinfo
     */
    public function killChild($signo, $signinfo)
    {
        if (!$this->child) {
            $this->logger->notice('No child to kill.');

            return;
        }

        $this->logger->notice('Killing child at {pid}', ['pid' => $this->child]);

        $command = escapeshellcmd($this->options['ps']);

        foreach ($this->options['ps_args'] as $arg) {
            $command .= ' ' . escapeshellarg($arg);
        }

        if (exec('ps ' . $this->child, $output, $returnCode) && $returnCode != 1) {
            $this->logger->notice('Killing child at ' . $this->child);
            posix_kill($this->child, SIGKILL);
            $this->child = null;
        } else {
            $this->logger->notice('Child ' . $this->child . ' not found, restarting.');
            $this->shutdown($signo, $signinfo);
        }
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not, remove them from Redis.
     *
     * This is a form of garbage collection to handle cases where the
     * server may have been killed and the Resque workers did not die gracefully
     * and therefore leave state information in Redis.
     */
    public function pruneDeadWorkers(): void
    {
        $pids = $this->resque->getWorkerPids();
        $ids  = $this->resque->getWorkerIds();

        foreach ($ids as $id) {
            $worker = clone $this;
            $worker->setId($id);

            [$host, $pid] = $worker->getLocation();

            // Ignore workers on other hosts
            if ($host != $this->options['server_name']) {
                continue;
            }

            // Ignore this process
            if ($pid == $this->options['pid']) {
                continue;
            }

            // Ignore workers still running
            if (in_array($pid, $pids)) {
                continue;
            }

            $this->logger->warning('Pruning dead worker: {id}', ['id' => $id]);
            $worker->unregister();
        }
    }

    /**
     * Gets the ID of this worker
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * Register this worker in Redis.
     */
    public function register(): void
    {
        $this->logger->debug('Registering worker ' . $this->getId());
        $this->resque->getClient()->sadd($this->resque->getKey(Resque::WORKERS_KEY), $this->getId());
        $this->resque->getClient()->set($this->getJobKey() . ':started', date("D M d H:i:s T Y", time()));
    }

    /**
     * Unregister this worker in Redis. (shutdown etc)
     */
    public function unregister(): void
    {
        $this->logger->debug('Unregistering worker ' . $this->getId());

        if ($this->currentJob) {
            $this->failJob($this->currentJob, new DirtyExitException());
        }

        $this->resque->getClient()->srem($this->resque->getKey(Resque::WORKERS_KEY), $this->getId());
        $this->resque->getClient()->del($this->getJobKey());
        $this->resque->getClient()->del($this->getJobKey() . ':started');

        $this->getStatistic('processed')->clear();
        $this->getStatistic('failed')->clear();
        $this->getStatistic('shutdown')->clear();
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param JobInterface $job Job instance we're working on.
     */
    public function workingOn(JobInterface $job): void
    {
        if (method_exists($job, 'setWorker')) {
            $job->setWorker($this);
        }

        $this->currentJob = $job;

        $this->resque->getStatusFactory()->forJob($job)->update(Status::STATUS_RUNNING);

        $data = json_encode(
            [
                'queue'   => $job->getQueue(),
                'run_at'  => date("D M d H:i:s T Y", time()),
                'payload' => $job->getPayload(),
            ]
        );

        $this->resque->getClient()->set($this->getJobKey(), $data);
    }

    /**
     * Notify Redis that we've finished working on a job, clearing the working
     * state and incrementing the job stats.
     */
    public function doneWorking(): void
    {
        $this->currentJob = null;
        $this->resque->getStatistic('processed')->incr();
        $this->getStatistic('processed')->incr();
        $this->resque->getClient()->del($this->getJobKey());
    }

    /**
     * Generate a string representation of this worker.
     *
     * @return string String identifier for this worker instance.
     */
    public function __toString()
    {
        return $this->id;
    }

    /**
     * Return an object describing the job this worker is currently working on.
     *
     * @return array Object with details of current job.
     */
    public function job(): array
    {
        $job = $this->resque->getClient()->get($this->getJobKey());

        if (!$job) {
            return [];
        } else {
            return json_decode($job, true);
        }
    }

    /**
     * Get a statistic belonging to this worker.
     *
     * @param string $name Statistic to fetch.
     *
     * @return Statistic
     */
    public function getStatistic(string $name)
    {
        return new Statistic($this->resque, $name . ':' . $this->getId());
    }

    /**
     * Inject the logging object into the worker
     *
     * @param LoggerInterface $logger
     *
     * @return void
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    /**
     * Configures options for the worker
     *
     * @param array<string,mixed> $options
     *                               Including
     *                               Worker identification
     *                               - server_name      => string, default is FQDN hostname
     *                               - pid              => int, default is current PID
     *                               - id_format        => string, suitable for sprintf
     *                               - id_location_preg => string, Perl compat regex, gets hostname and PID
     *                               out of worker ID
     *                               - shuffle_queues   => bool, whether to shuffle the queues on reserve, so we evenly
     *                               check all queues
     *                               - sort_queues      => bool, whether to check the queues in alphabetical order
     *                               (mutually exclusive with shuffle_queues)
     *                               - no_fork          => bool, whether to suppress fork if available
     */
    protected function configure(array $options): void
    {
        $this->options = array_merge(
            [
                'server_name'      => null,
                'pid'              => null,
                'ps'               => '/bin/ps',
                'ps_args'          => ['-o', 'pid,state', '-p'],
                'id_format'        => '%s:%d:%s',
                'id_location_preg' => '/^([^:]+?):([0-9]+):/',
                'shuffle_queues'   => true,
                'sort_queues'      => false,
                'pre_perform'      => false,
                'post_perform'     => false,
                'no_fork'          => false,
            ],
            $options
        );

        if (!$this->options['server_name']) {
            $this->options['server_name'] = function_exists('gethostname') ? gethostname() : php_uname('n');
        }

        if (!$this->options['pid']) {
            $this->options['pid'] = getmypid();
        }
    }

    /**
     * Configures the ID of this worker
     */
    protected function configureId(): void
    {
        $this->id = sprintf(
            $this->options['id_format'],
            $this->options['server_name'],
            $this->options['pid'],
            implode(',', $this->queues)
        );
    }

    /**
     * @return Client\ClientInterface //fake interface! instances will not actually implement this
     */
    protected function getClient()
    {
        return $this->resque->getClient();
    }

    /**
     * @throws JobInvalidException
     * @throws JobClassNotFoundException
     */
    protected function createJobInstance(string $queue, array $payload): JobInterface
    {
        if (!class_exists($payload['class'])) {
            throw new JobClassNotFoundException(
                'Could not find job class ' . $payload['class'] . '.'
            );
        }

        if (!is_subclass_of($payload['class'], 'Resque\JobInterface')) {
            throw new JobInvalidException();
        }

        $job = new $payload['class']($queue, $payload);

        if (method_exists($job, 'setResque')) {
            $job->setResque($this->resque);
        }

        return $job;
    }

    /**
     * Gets the key for where this worker will store its active job
     */
    protected function getJobKey(): string
    {
        return $this->getResque()->getKey('worker:' . $this->getId());
    }

    /**
     * Parses a hostname and PID out of a string worker ID
     *
     * If you change the format of the ID, you should also change the definition
     * of this method.
     *
     * This method *always* parses the ID of the worker, rather than figuring out
     * the current processes' PID/hostname. This means you can use setId() to
     * interrogate the properties of other workers given their ID.
     *
     * @return int[]
     * @throws Exception
     */
    protected function getLocation(): array
    {
        $matches = [];

        if (!preg_match($this->options['id_location_preg'], $this->getId(), $matches)) {
            throw new Exception('Incompatible ID format: unable to determine worker location');
        }

        if (!isset($matches[1]) || !$matches[1]) {
            throw new Exception('Invalid ID: invalid hostname');
        }

        if (!isset($matches[2]) || !$matches[2] || !is_numeric($matches[2])) {
            throw new Exception('Invalid ID: invalid PID');
        }

        return [$matches[1], (int)$matches[2]];
    }

    /**
     * Marks the given job as failed
     *
     * This happens whenever the job's perform() method emits an exception
     *
     * @param JobInterface|array $job
     * @param Exception          $exception
     */
    protected function failJob($job, Exception $exception)
    {
        if ($job instanceof JobInterface) {
            $payload = $job->getPayload();
            $queue   = $job->getQueue();
        } else {
            $payload = $job;
            $queue   = isset($job['queue']) ? $job['queue'] : null;
        }

        $id = isset($job['id']) ? $job['id'] : null;

        if ($id) {
            try {
                $status = $this->resque->getStatusFactory()->forId($id);
                $status->update(Status::STATUS_FAILED);
            } catch (JobIdException $e) {
                $this->logger->warning($e);
            }
        } // else no status to update

        $this->resque->getFailureBackend()->receiveFailure(
            $payload,
            $exception,
            $this,
            $queue
        );

        $this->getResque()->getStatistic('failed')->incr();
        $this->getStatistic('failed')->incr();
    }

    protected function performJob(JobInterface $job): void
    {
        $job->perform();
    }

    /**
     * Prepares the list of queues for a job to reserved
     *
     * Updates/sorts/shuffles the array ahead of the call to reserve a job from one of them
     */
    protected function refreshQueues(): void
    {
        if ($this->refreshQueues) {
            $this->queues = $this->resque->queues();
        }

        if (!$this->queues) {
            if ($this->refreshQueues) {
                $this->logger->info('Refreshing queues dynamically, but there are no queues yet');
            } else {
                $this->logger->notice('Not listening to any queues, and dynamic queue refreshing is disabled');
                $this->shutdownNow(self::DEFAULT_SIGNO, null);
            }
        }

        // Each call to reserve, we check the queues in a different order
        if ($this->options['shuffle_queues']) {
            shuffle($this->queues);
        } elseif ($this->options['sort_queues']) {
            sort($this->queues);
        }
    }

    /**
     * Perform necessary actions to start a worker.
     */
    protected function startup(): void
    {
        $this->registerSigHandlers();
        $this->pruneDeadWorkers();
        $this->register();
    }

    /**
     * On supported systems (with the PECL proctitle module installed), update
     * the name of the currently running process to indicate the current state
     * of a worker.
     *
     * @param string $status The updated process title.
     */
    protected function updateProcLine(string $status): void
    {
        if (function_exists('setproctitle')) {
            setproctitle('resque-' . Version::VERSION . ': ' . $status);
        }
    }

    /**
     * Register signal handlers that a worker should respond to.
     *
     * TERM: Shutdown immediately and stop processing jobs.
     * INT:  Shutdown immediately and stop processing jobs.
     * QUIT: Shutdown after the current job finishes processing.
     * USR1: Kill the forked child immediately and continue processing jobs.
     * USR2: Pause processing jobs.
     * CONT: Resume progressing jobs.
     * PIPE: Reestablish the redis connection to resolve conenction issues.
     */
    protected function registerSigHandlers(): void
    {
        if (!function_exists('pcntl_signal')) {
            $this->logger->warning('Cannot register signal handlers');

            return;
        }

        declare(ticks=1);
        pcntl_signal(SIGTERM, [$this, 'shutDownNow']);
        pcntl_signal(SIGINT, [$this, 'shutDownNow']);
        pcntl_signal(SIGQUIT, [$this, 'shutdown']);
        pcntl_signal(SIGUSR1, [$this, 'killChild']);
        pcntl_signal(SIGUSR2, [$this, 'pauseProcessing']);
        pcntl_signal(SIGCONT, [$this, 'unPauseProcessing']);
        pcntl_signal(SIGPIPE, [$this, 'reestablishRedisConnection']);

        $this->logger->notice('Registered signals');
    }

    /**
     * Attempt to fork a child process from the parent to run a job in.
     *
     * Return values are those of pcntl_fork().
     *
     * @return bool|int -1 if the fork failed, 0 for the forked child, the PID of the child for the parent.
     * @throws Exception
     * @throws RuntimeException
     */
    private function fork()
    {
        if ($this->options['no_fork']) {
            $this->logger->notice('Forking disabled');

            return false;
        }

        if (!function_exists('pcntl_fork')) {
            $this->logger->warning('Using non fork version!');

            return false;
        }

        // Immediately before a fork, disconnect the redis client
        $this->resque->disconnect();

        $this->logger->notice('Forking...');

        $pid = (int)pcntl_fork();

        // And reconnect
        $this->resque->connect();

        if ($pid === -1) {
            throw new RuntimeException('Unable to fork child worker.');
        }

        return $pid;
    }
}
