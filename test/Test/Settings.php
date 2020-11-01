<?php

declare(strict_types=1);

namespace Resque\Test;

use InvalidArgumentException;
use Predis\Client as PredisClient;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Resque\Client\ClientInterface;
use Resque\Client\CredisClient as CredisClient;
use RuntimeException;

use const DIRECTORY_SEPARATOR;

class Settings implements LoggerAwareInterface
{
    /** @var string */
    protected $clientType;

    /** @var string */
    protected $host;

    /** @var string */
    protected $bind;

    /** @var string */
    protected $port;

    /** @var int */
    protected $db;

    /** @var string */
    protected $prefix;

    /** @var string */
    protected $buildDir;

    /** @var LoggerInterface */
    protected $logger;

    /** @var int */
    protected $testPid;

    public function __construct()
    {
        $this->testPid = getmypid();
        $this->fromDefaults();
    }

    protected function fromDefaults()
    {
        $this->buildDir   = __DIR__ . '/../../build';
        $this->port       = '6379';
        $this->host       = 'localhost';
        $this->bind       = '127.0.0.1';
        $this->db         = 0;
        $this->prefix     = '';
        $this->clientType = 'predis';
    }

    public function fromEnvironment()
    {
        $env = [
            'client_type' => 'clientType',
            'host'        => 'host',
            'port'        => 'port',
            'bind'        => 'bind',
            'build_dir'   => 'buildDir',
            'run'         => 'run',
            'db'          => 'db',
            'prefix'      => 'prefix',
        ];

        foreach ($env as $var => $setting) {
            $name = 'RESQUE_' . strtoupper($var);

            if (isset($_SERVER[$name])) {
                $this->$setting = $_SERVER[$name];
            }
        }
    }

    public function setBuildDir(string $dir)
    {
        $this->buildDir = $dir;
    }

    public function startRedis(): void
    {
        $this->dumpRedisConfig();
        $this->registerShutdown();

        $this->logger->notice('Starting redis server in {buildDir}', ['buildDir' => $this->buildDir]);
        exec('cd ' . $this->buildDir . '; redis-server ' . $this->buildDir . '/redis.conf', $output, $return);
        usleep(500000);

        if ($return != 0) {
            throw new RuntimeException('Cannot start redis-server');
        }
    }

    protected function getRedisConfig(): array
    {
        return [
            'daemonize'  => 'yes',
            'pidfile'    => './redis.pid',
            'port'       => $this->port,
            'bind'       => $this->bind,
            'timeout'    => 0,
            'dbfilename' => 'dump.rdb',
            'dir'        => $this->buildDir,
            'loglevel'   => 'debug',
            'logfile'    => './redis.log',
        ];
    }

    /**
     * @return ClientInterface //fake interface to represent expected methods on the actually returned clients
     * @throws InvalidArgumentException
     */
    public function getClient()
    {
        switch ($this->clientType) {
            case 'predis':
                return new PredisClient(
                    [
                        'host'   => $this->host,
                        'port'   => $this->port,
                        'db'     => $this->db,
                        'prefix' => $this->prefix,
                    ]
                );
            case 'phpiredis':
                return new PredisClient(
                    [
                        'host'   => $this->host,
                        'port'   => $this->port,
                        'db'     => $this->db,
                        'prefix' => $this->prefix,
                    ], [
                        'tcp'  => 'Predis\Connection\PhpiredisStreamConnection',
                        'unix' => 'Predis\Connection\PhpiredisSocketConnection',
                    ]
                );
            case 'credis':
            case 'phpredis':
                $client = new CredisClient($this->host, $this->port);
                $client->setCloseOnDestruct(false);

                return $client;
            default:
                throw new InvalidArgumentException('Invalid or unknown client type: ' . $this->clientType);
        }
    }

    public function checkBuildDir(): void
    {
        if (!is_dir($this->buildDir)) {
            mkdir($this->buildDir);
        }

        if (!is_dir($this->buildDir)) {
            throw new RuntimeException('Could not create build dir: ' . $this->buildDir);
        }
    }

    protected function dumpRedisConfig(): void
    {
        $file = $this->buildDir . '/redis.conf';
        $conf = '';

        foreach ($this->getRedisConfig() as $name => $value) {
            $conf .= "$name $value\n";
        }

        $this->logger->info('Dumping redis config {config} to {file}', ['file' => $file, 'config' => $conf]);
        file_put_contents($file, $conf);
    }

    /**
     * Override INT and TERM signals, so they do a clean shutdown and also
     * clean up redis-server as well.
     */
    public function catchSignals(): void
    {
        if (function_exists('pcntl_signal')) {
            $self = $this;
            pcntl_signal(
                SIGINT,
                function () use ($self) {
                    $self->logger->debug('SIGINT received');
                    exit;
                }
            );

            pcntl_signal(
                SIGTERM,
                function () use ($self) {
                    $self->logger->debug('SIGTERM received');
                    exit;
                }
            );
        }
    }

    public function killRedis(): void
    {
        $pid = getmypid();

        $this->logger->notice('Attempting to kill redis from {pid}', ['pid' => $pid]);

        if ($pid === null || $this->testPid !== $pid) {
            $this->logger->warning('Refusing to kill redis from forked worker');

            return; // don't kill from a forked worker
        }

        $pidFile = $this->buildDir . '/redis.pid';

        if (file_exists($pidFile)) {
            $pid = trim(file_get_contents($pidFile));
            posix_kill((int)$pid, 9);

            if (is_file($pidFile)) {
                unlink($pidFile);
            }
        }

        $filename = $this->buildDir . '/dump.rdb';

        if (is_file($filename)) {
            unlink($filename);
        }
    }

    protected function registerShutdown(): void
    {
        $this->logger->info('Registered shutdown function');
        register_shutdown_function([$this, 'killRedis']);
    }

    /**
     * Sets a logger instance on the object
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * Dumps configuration to a file
     *
     * @return void
     */
    public function dumpConfig()
    {
        $file   = $this->buildDir . DIRECTORY_SEPARATOR . 'settings.json';
        $config = json_encode(get_object_vars($this), JSON_PRETTY_PRINT);

        $this->logger->info('Dumping test config {config} to {file}', ['file' => $file, 'config' => $config]);
        file_put_contents($file, $config);
    }
}
