<?php

declare(strict_types=1);

namespace Resque\Job;

use InvalidArgumentException;
use LogicException;
use Resque\Resque;

/**
 * Status tracker/information for a job
 *
 * Modified from the original Resque\Status tracker to use a hash; preventing
 * race conditions in updating a single property of the status.
 *
 * @author        Dominic Scheirlinck <dominic@vendhq.com>
 * @author        Chris Boulton <chris.boulton@interspire.com>
 * @copyright (c) 2010 Chris Boulton
 * @license       http://www.opensource.org/licenses/mit-license.php
 */
class Status
{
    /**#@+
     * How many seconds until a status entry should expire
     *
     * In the previous implementation, incomplete statuses were not given a
     * TTL at all. This can make Redis treat the keys differently, depending
     * on your maxmemory-policy (for example, volative-lru will only remove
     * keys with an expire set).
     *
     * @var int
     */
    public const COMPLETE_TTL   = 86400;    // 24 hours
    public const INCOMPLETE_TTL = 604800;   // A week
    /**#@-*/

    /**#@+
     * Status codes
     *
     * @var int
     */
    public const STATUS_WAITING   = 1;
    public const STATUS_RUNNING   = 2;
    public const STATUS_FAILED    = 3;
    public const STATUS_COMPLETE  = 4;
    public const STATUS_RECREATED = 5;

    private const ATTRIBUTE_STATUS  = 'status';
    private const ATTRIBUTE_CREATED = 'created';
    private const ATTRIBUTE_UPDATED = 'updated';
    /**#@-*/

    /**
     * An array of valid statuses
     *
     * @var string[]
     */
    public static $valid = [
        self::STATUS_WAITING   => 'waiting',
        self::STATUS_RUNNING   => 'running',
        self::STATUS_FAILED    => 'failed',
        self::STATUS_COMPLETE  => 'complete',
        self::STATUS_RECREATED => 'recreated',
    ];

    /**
     * An array of complete statuses
     *
     * @var int[]
     */
    public static $complete = [
        self::STATUS_FAILED,
        self::STATUS_COMPLETE,
        self::STATUS_RECREATED,
    ];

    /**
     * @var string The ID of the job this status class refers back to.
     */
    protected $id;

    /**
     * Whether the status has been loaded from the database
     *
     * @var bool
     */
    protected $loaded = false;

    /**
     * @var mixed[]
     */
    protected $attributes = [];

    /**
     * @var \Resque\Client\ClientInterface
     */
    protected $client;

    /**
     * @var bool|null  Cache variable if the status of this job is being
     *                     monitored or not. True/false when checked at least
     *                     once or null if not checked yet.
     */
    protected $isTracking = null;

    /**
     * Setup a new instance of the job monitor class for the supplied job ID.
     *
     * @param string $id The ID of the job to manage the status for.
     * @param Resque $resque
     */
    public function __construct(string $id, Resque $resque)
    {
        $this->id     = $id;
        $this->client = $resque->getClient();
    }

    public function getId(): string
    {
        return $this->id;
    }

    /**
     * Create a new status monitor item for the supplied job ID. Will create
     * all necessary keys in Redis to monitor the status of a job.
     */
    public function create(): void
    {
        $this->isTracking = true;

        $this->setAttributes(
            [
                self::ATTRIBUTE_STATUS  => self::STATUS_WAITING,
                self::ATTRIBUTE_CREATED => time(),
                self::ATTRIBUTE_UPDATED => time(),
            ]
        );
    }

    /**
     * Sets all the given attributes
     *
     * @param mixed[] $attributes
     *
     * @return mixed
     */
    public function setAttributes(array $attributes)
    {
        $this->attributes = array_merge($this->attributes, $attributes);

        $set = [];
        foreach ($attributes as $name => $value) {
            if ($name == self::ATTRIBUTE_STATUS) {
                $this->update($value);
                continue;
            }
            $set[$name] = $value;
        }

        return call_user_func([$this->client, 'hmset'], $this->getHashKey(), $set);
    }

    /**
     * @param mixed $value
     */
    public function setAttribute(string $name, $value)
    {
        if ($name == self::ATTRIBUTE_STATUS) {
            $this->update($value);
        } else {
            $this->attributes[$name] = $value;
            $this->client->hmset(
                $this->getHashKey(),
                [
                    $name                   => $value,
                    self::ATTRIBUTE_UPDATED => time(),
                ]
            );
        }
    }

    /**
     * Increments an attribute
     *
     * The attribute should be an int field
     *
     * @return int The value after incrementing (see hincrby)
     */
    public function incrementAttribute(string $name, int $by = 1)
    {
        $pipeline = $this->client->pipeline();
        $pipeline->hincrby($this->getHashKey(), $name, $by);
        $pipeline->hset($this->getHashKey(), self::ATTRIBUTE_UPDATED, time());
        $result = $pipeline->execute();

        return $this->attributes[$name] = $result[0];
    }

    /**
     * Update the status indicator for the current job with a new status.
     *
     * This method is called from setAttribute/s so that the expiry can be
     * properly updated.
     *
     * @param int $status The status of the job (see constants in Resque\Job\Status)
     *
     * @return bool
     * @throws InvalidArgumentException
     */
    public function update(int $status): bool
    {
        if (!isset(self::$valid[$status])) {
            throw new InvalidArgumentException('Invalid status');
        }

        if (!$this->isTracking()) {
            return false;
        }

        $this->attributes[self::ATTRIBUTE_STATUS]  = $status;
        $this->attributes[self::ATTRIBUTE_UPDATED] = time();

        $success = $this->client->hmset(
            $this->getHashKey(),
            [
                self::ATTRIBUTE_STATUS  => $this->attributes[self::ATTRIBUTE_STATUS],
                self::ATTRIBUTE_UPDATED => $this->attributes[self::ATTRIBUTE_UPDATED],
            ]
        );

        // Delete completed jobs and set expire times for the rest.
        if ($status == self::STATUS_COMPLETE) {
            $this->client->del($this->getHashKey());
        } elseif (in_array($status, self::$complete)) {
            $this->client->expire($this->getHashKey(), self::COMPLETE_TTL);
        } else {
            $this->client->expire($this->getHashKey(), self::INCOMPLETE_TTL);
        }

        return (bool)$success;
    }

    /**
     * Check if we're actually checking the status of the loaded job status
     * instance.
     *
     * @return bool True if the status is being monitored, false if not.
     */
    public function isTracking(): bool
    {
        if ($this->isTracking === null) {
            $this->isTracking = (bool)$this->client->exists($this->getHashKey());
            if ($this->isTracking) {
                $this->load();
            }
        }

        return $this->isTracking;
    }

    /**
     * Loads all status attributes
     *
     * @throws LogicException
     */
    public function load()
    {
        if ($this->loaded) {
            throw new LogicException('The status is already loaded. Use another instance.');
        }

        $this->attributes = array_merge($this->attributes, $this->client->hgetall($this->getHashKey()));
        $this->loaded     = true;
    }

    /**
     * @return mixed[]
     */
    public function getAll()
    {
        if ($this->loaded) {
            return $this->attributes;
        }

        return $this->client->hgetall($this->getHashKey());
    }

    /**
     * Gets the time this status was updated
     */
    public function getUpdated(): ?int
    {
        $updated = $this->getAttribute(self::ATTRIBUTE_UPDATED);
        return $updated ? (int)$updated : null;
    }

    /**
     * Gets the time this status was created
     */
    public function getCreated(): ?int
    {
        $created = $this->getAttribute(self::ATTRIBUTE_CREATED);
        return $created ? (int)$created : null;
    }

    /**
     * Fetch the status for the job being monitored.
     *
     * For consistency, this would be called getStatus(), but for BC, it's
     * just get().
     */
    public function get(): ?int
    {
        $status = $this->getAttribute(self::ATTRIBUTE_STATUS);
        return $status ? (int)$status : null;
    }

    public function getStatus(): string
    {
        $status = $this->get();

        return self::$valid[$status] ?? 'unknown';
    }

    /**
     * Gets a single attribute value
     *
     * @param string $name
     * @param mixed  $default
     *
     * @return mixed
     */
    public function getAttribute(string $name, $default = null)
    {
        if ($this->loaded) {
            return isset($this->attributes[$name]) ? $this->attributes[$name] : $default;
        }

        // Could be just hget, but Credis will return false?!
        $attributes = $this->client->hGetAll($this->getHashKey());

        return isset($attributes[$name]) ? $attributes[$name] : $default;
    }

    /**
     * Stop tracking the status of a job.
     */
    public function stop(): void
    {
        $this->client->del($this->getHashKey());
    }

    /**
     * A new key, because we're now using a hash format to store the status
     *
     * Used from outside this class to do status processing more efficiently
     */
    public function getHashKey(): string
    {
        return 'job:' . $this->id . ':status/hash';
    }

    /**
     * Accessor to return valid statuses
     *
     * @return int[]
     */
    public function getValid()
    {
        return self::$valid;
    }

    /**
     * Accessor to return complete statuses
     *
     * @return int[]
     */
    public function getComplete()
    {
        return self::$complete;
    }

    /**
     * Convenience method to to check if a resque job has a complete status
     */
    public function isComplete(): bool
    {
        return in_array($this->get(), self::$complete);
    }
}
