<?php

namespace Resque;

/**
 * Resque statistic management (jobs processed, failed, etc)
 *
 * @license		http://www.opensource.org/licenses/mit-license.php
 */
class Statistic
{
    const KEY = 'stat:';

    protected $resque;
    protected $statistic;

    /**
     * Constructor
     *
     * @param Client $client A Redis client
     * @param string $statistic
     */
    public function __construct(Resque $resque, $statistic)
    {
        $this->resque = $resque;
        $this->statistic = $statistic;
    }

    /**
     * Gets the key for a statistic
     *
     * @return string
     */
    protected function getKey()
    {
        return self::KEY . $this->statistic;
    }

	/**
	 * Get the value of the supplied statistic counter for the specified statistic.
	 *
	 * @param string $stat The name of the statistic to get the stats for.
	 * @return mixed Value of the statistic.
	 */
	public function get()
	{
		return (int)$this->resque->getClient()->get($this->getKey());
	}

	/**
	 * Increment the value of the specified statistic by a certain amount (default is 1)
	 *
	 * @param string $stat The name of the statistic to increment.
	 * @param int $by The amount to increment the statistic by.
	 * @return boolean True if successful, false if not.
	 */
	public function incr($by = 1)
	{
		return (bool)$this->resque->getClient()->incrby($this->getKey(), $by);
	}

	/**
	 * Decrement the value of the specified statistic by a certain amount (default is 1)
	 *
	 * @param string $stat The name of the statistic to decrement.
	 * @param int $by The amount to decrement the statistic by.
	 * @return boolean True if successful, false if not.
	 */
	public function decr($by = 1)
	{
		return (bool)$this->resque->getClient()->decrby($this->getKey(), $by);
	}

	/**
	 * Delete a statistic with the given name.
	 *
	 * @param string $stat The name of the statistic to delete.
	 * @return boolean True if successful, false if not.
	 */
	public function clear()
	{
		return (bool)$this->resque->getClient()->del($this->getKey());
	}
}