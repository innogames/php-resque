<?php

declare(strict_types=1);

namespace Resque\Console;

use Resque\Client\ClientInterface;
use Resque\Console\Helper\LoggerHelper;
use Resque\Resque;
use Symfony\Component\Console\Command\Command as CommandComponent;
use Symfony\Component\Console\Logger\ConsoleLogger;
use Symfony\Component\Console\Output\OutputInterface;

abstract class Command extends CommandComponent
{
    /**
     * @return ClientInterface
     */
    public function getRedis()
    {
        return $this->getHelper('redis')->getClient();
    }

    public function getResque(OutputInterface $output): Resque
    {
        $resque = new Resque($this->getRedis());

        if ($helper = $this->getHelper('logger')) {
            /* @var LoggerHelper $helper */
            $resque->setLogger($helper->getLogger());
        } else {
            $resque->setLogger(new ConsoleLogger($output));
        }

        return $resque;
    }
}
