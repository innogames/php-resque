<?php

declare(strict_types=1);

namespace Resque\Console\Helper;

use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Helper\Helper;

/**
 * Logger CLI helper
 */
class LoggerHelper extends Helper
{
    /**
     * @var LoggerInterface
     */
    protected $logger;

    public function __construct(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    public function getName(): string
    {
        return 'logger';
    }
}
