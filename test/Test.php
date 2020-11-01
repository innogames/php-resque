<?php

declare(strict_types=1);

namespace Resque;

use LogicException;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Resque\Client\ClientInterface;
use Resque\Test\Settings;

abstract class Test extends TestCase
{
    /** @var Settings */
    protected static $settings = null;

    /** @var Resque */
    protected $resque;

    /** @var ClientInterface */
    protected $redis;

    /** @var LoggerInterface */
    protected $logger;

    public static function setSettings(Settings $settings)
    {
        self::$settings = $settings;
    }

    public function setUp(): void
    {
        if (!self::$settings) {
            throw new LogicException('You must supply the test case with a settings instance');
        }

        $this->redis = self::$settings->getClient();
        $this->redis->flushdb();

        $this->logger = self::$settings->getLogger();

        $this->resque = new Resque($this->redis);
        $this->resque->setLogger($this->logger);
    }

    public function tearDown(): void
    {
        if ($this->redis) {
            $this->redis->flushdb();

            if ($this->redis->isConnected()) {
                $this->logger->notice('Shutting down connected Redis instance in tearDown()');
                $this->redis->disconnect();
            }
        }
    }

    protected function getWorker($queues): Worker
    {
        $worker = new Worker($this->resque, $queues);
        $worker->setLogger($this->logger);
        $worker->register();

        return $worker;
    }
}
