<?php

declare(strict_types=1);

namespace Resque;

/**
 * Statistic tests.
 *
 * @package        Resque/Tests
 * @author        Chris Boulton <chris@bigcommerce.com>
 * @license        http://www.opensource.org/licenses/mit-license.php
 */
class StatisticTest extends Test
{
    /** @var Statistic */
    protected $statistic;

    public function setUp(): void
    {
        parent::setUp();

        $this->statistic = new Statistic($this->resque, __CLASS__);
    }

    public function tearDown(): void
    {
        if ($this->statistic) {
            $this->statistic->clear();
            $this->statistic = null;
        }

        parent::tearDown();
    }

    protected function assertStatisticValueByClient($value, $message = ''): void
    {
        $this->assertEquals($value, $this->redis->get('resque:stat:' . __CLASS__), $message);
    }

    public function testStatCanBeIncremented(): void
    {
        $this->statistic->incr();
        $this->statistic->incr();
        $this->assertStatisticValueByClient(2);
    }

    public function testStatCanBeIncrementedByX(): void
    {
        $this->statistic->incr(10);
        $this->statistic->incr(11);
        $this->assertStatisticValueByClient(21);
    }

    public function testStatCanBeDecremented(): void
    {
        $this->statistic->incr(22);
        $this->statistic->decr();
        $this->assertStatisticValueByClient(21);
    }

    public function testStatCanBeDecrementedByX(): void
    {
        $this->statistic->incr(22);
        $this->statistic->decr(11);
        $this->assertStatisticValueByClient(11);
    }

    public function testGetStatByName(): void
    {
        $this->statistic->incr(100);
        $this->assertEquals(100, $this->statistic->get());
    }

    public function testGetUnknownStatReturns0(): void
    {
        $statistic = new Statistic($this->resque, 'some_unknown_statistic');
        $this->assertEquals(0, $statistic->get());
    }
}
