<?php

declare(strict_types=1);

namespace Resque;

/**
 * Resque statistic management (jobs processed, failed, etc)
 *
 * @package        Resque/Statistic
 * @author         Chris Boulton <chris@bigcommerce.com>
 * @license        http://www.opensource.org/licenses/mit-license.php
 */
class Statistic
{
    private const KEY = 'stat:';

    protected $resque;
    protected $statistic;

    public function __construct(Resque $resque, string $statistic)
    {
        $this->resque    = $resque;
        $this->statistic = $statistic;
    }

    /**
     * Gets the key for a statistic
     */
    public function getKey(): string
    {
        return $this->resque->getKey(self::KEY . $this->statistic);
    }

    /**
     * Get the value of the supplied statistic counter for the specified statistic.
     */
    public function get(): int
    {
        return (int)$this->resque->getClient()->get($this->getKey());
    }

    /**
     * Increment the value of the specified statistic by a certain amount (default is 1)
     *
     * @param int $by The amount to increment the statistic by.
     *
     * @return bool True if successful, false if not.
     */
    public function incr(int $by = 1): bool
    {
        return (bool)$this->resque->getClient()->incrby($this->getKey(), $by);
    }

    /**
     * Decrement the value of the specified statistic by a certain amount (default is 1)
     *
     * @param int $by The amount to decrement the statistic by.
     *
     * @return bool True if successful, false if not.
     */
    public function decr(int $by = 1): bool
    {
        return (bool)$this->resque->getClient()->decrby($this->getKey(), $by);
    }

    /**
     * Delete a statistic with the given name.
     *
     * @return bool True if successful, false if not.
     */
    public function clear(): bool
    {
        return (bool)$this->resque->getClient()->del($this->getKey());
    }
}
