<?php

declare(strict_types=1);

namespace Resque;

use InvalidArgumentException;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;
use Resque\Client\ClientInterface;
use Resque\Failure\BackendInterface;
use Resque\Failure\RedisBackend;
use Resque\Job\Status;
use Resque\Job\StatusFactory;

use function is_array;

/**
 * Base Resque class
 *
 * @license http://www.opensource.org/licenses/mit-license.php
 */
class Resque implements LoggerAwareInterface
{
    /**#@+
     * Protocol keys
     *
     * @var string
     */
    public const QUEUE_KEY   = 'queue:';
    public const QUEUES_KEY  = 'queues';
    public const WORKERS_KEY = 'workers';
    /**#@-*/

    /**
     * @var ClientInterface
     */
    private $client;

    /**
     * @var mixed[]
     */
    protected $options;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var BackendInterface
     */
    protected $failures;

    /**
     * @var StatusFactory
     */
    protected $statuses;

    /**
     * Constructor
     *
     * @param ClientInterface $client //fake interface, instances will not actually implement this!
     */
    public function __construct($client, array $options = [])
    {
        $this->client = $client;
        $this->logger = new NullLogger();

        $this->configure($options);
    }

    /**
     * Configures the options of the resque background queue system
     *
     * @param mixed[] $options
     */
    public function configure(array $options): void
    {
        $this->options = array_merge(
            [
                'pgrep'           => 'pgrep -f',
                'pgrep_pattern'   => '[r]esque[^-]',
                'prefix'          => 'resque:',
                'statistic_class' => 'Resque\Statistic',
            ],
            $options
        );
    }

    /**
     * Gets the underlying Redis client
     *
     * The Redis client can be any object that implements a suitable subset
     * of Redis commands.
     *
     * @return ClientInterface
     */
    public function getClient()
    {
        return $this->client;
    }

    public function setFailureBackend(BackendInterface $backend)
    {
        $this->failures = $backend;
    }

    public function getFailureBackend(): BackendInterface
    {
        if (!isset($this->failures)) {
            $this->failures = new RedisBackend();
        }

        return $this->failures;
    }

    public function setStatusFactory(StatusFactory $factory)
    {
        $this->statuses = $factory;
    }

    public function getStatusFactory(): StatusFactory
    {
        if (!isset($this->statuses)) {
            $this->statuses = new StatusFactory($this);
        }

        return $this->statuses;
    }

    /**
     * Causes the client to reconnect to the Redis server
     */
    public function reconnect(): void
    {
        if ($this->client->isConnected()) {
            $this->client->disconnect();
        }
        $this->client->connect();
    }

    /**
     * Causes the client to connect to the Redis server
     */
    public function connect(): void
    {
        $this->client->connect();
    }

    /**
     * Disconnects the client from the Redis server
     */
    public function disconnect(): void
    {
        $this->client->disconnect();
    }

    public function log(string $message, string $priority = LogLevel::INFO): void
    {
        $this->logger->log($message, $priority);
    }

    /**
     * Gets a namespaced/prefixed key for the given key suffix
     */
    public function getKey(string $key): string
    {
        return $this->options['prefix'] . $key;
    }

    /**
     * Push a job to the end of a specific queue. If the queue does not
     * exist, then create it as well.
     *
     * @param string $queue The name of the queue to add the job to.
     * @param array  $item  Job description as an array to be JSON encoded.
     */
    public function push(string $queue, array $item)
    {
        // Add the queue to the list of queues
        $this->getClient()->sadd($this->getKey(self::QUEUES_KEY), $queue);

        // Add the job to the specified queue
        $this->getClient()->rpush($this->getKey(self::QUEUE_KEY . $queue), json_encode($item));
    }

    /**
     * Pop an item off the end of the specified queue, decode it and
     * return it.
     *
     * @param string $queue The name of the queue to fetch an item from.
     *
     * @return array|null Decoded item from the queue.
     */
    public function pop(string $queue): ?array
    {
        $item = $this->getClient()->lpop($this->getKey(self::QUEUE_KEY . $queue));

        if (!$item) {
            return null;
        }

        return json_decode($item, true);
    }

    /**
     * Clears the whole of a queue
     */
    public function clearQueue(string $queue)
    {
        $this->getClient()->del($this->getKey(self::QUEUE_KEY . $queue));
    }

    /**
     * Return the size (number of pending jobs) of the specified queue.
     *
     * @param string $queue name of the queue to be checked for pending jobs
     *
     * @return int The size of the queue.
     */
    public function size(string $queue): int
    {
        return $this->getClient()->llen($this->getKey(self::QUEUE_KEY . $queue));
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param string  $queue       The name of the queue to place the job in.
     * @param string  $class       The name of the class that contains the code to execute the job.
     * @param null    $args        Any optional arguments that should be passed when the job is executed.
     * @param bool $trackStatus Set to true to be able to monitor the status of a job.
     *
     * @return string
     * @throws InvalidArgumentException
     */
    public function enqueue(string $queue, string $class, ?array $args = null, bool $trackStatus = false): string
    {
        $id = md5(uniqid('', true));

        $this->push(
            $queue,
            [
                'class' => $class,
                'args'  => $args,
                'id'    => $id,
            ]
        );

        if ($trackStatus) {
            $status = new Status($id, $this);
            $status->create();
        }

        return $id;
    }

    /**
     * Get an array of all known queues.
     *
     * @return string[]
     */
    public function queues(): array
    {
        return $this->getSetMembers(self::QUEUES_KEY);
    }

    /**
     * Gets an array of all known worker IDs
     *
     * @return string[]
     */
    public function getWorkerIds(): array
    {
        return $this->getSetMembers(self::WORKERS_KEY);
    }

    public function workerExists(string $id): bool
    {
        return in_array($id, $this->getWorkerIds());
    }

    /**
     * Return an array of process IDs for all of the Resque workers currently
     * running on this machine.
     *
     * Expects pgrep to be in the path, and for it to inspect full argument
     * lists using -f
     *
     * @return int[] Array of Resque worker process IDs.
     */
    public function getWorkerPids(): array
    {
        $command = $this->options['pgrep'] . ' ' . escapeshellarg($this->options['pgrep_pattern']);

        $pids   = [];
        $output = null;
        $return = null;

        exec($command, $output, $return);

        /*
         * Exit codes:
         *   0 One or more processes were matched
         *   1 No processes were matched
         *   2 Invalid options were specified on the command line
         *   3 An internal error occurred
         */
        if (($return !== 0 && $return !== 1) || empty($output) || !is_array($output)) {
            $this->logger->warning('Unable to determine worker PIDs');

            return [];
        }

        foreach ($output as $line) {
            $line = explode(' ', trim($line), 2);

            if (!$line[0] || !is_numeric($line[0])) {
                continue;
            }

            $pids[] = (int)$line[0];
        }

        return $pids;
    }

    public function getStatistic(string $name): Statistic
    {
        return new Statistic($this, $name);
    }

    public function setLogger(LoggerInterface $logger): void
    {
        $this->logger = $logger;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * @param string $suffix Partial key (don't pass to getKey() - let this method do it for you)
     *
     * @return string[]
     */
    protected function getSetMembers(string $suffix): array
    {
        $members = $this->getClient()->smembers($this->getKey($suffix));

        if (!is_array($members)) {
            $members = [];
        }

        return $members;
    }
}
