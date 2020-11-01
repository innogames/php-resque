<?php

declare(strict_types=1);

namespace Resque;

class ResqueTest extends Test
{
    public function testInvalidWorkerDoesNotExist()
    {
        $this->assertFalse($this->resque->workerExists('blah'));
    }

    public function testEmptyWorkerIds()
    {
        $this->assertIsArray($this->resque->getWorkerIds());
    }

    public function testEmptyWorkerPids()
    {
        $this->assertIsArray($this->resque->getWorkerPids());
    }
}
