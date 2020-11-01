<?php

declare(strict_types=1);

namespace Resque\Console;

use Resque\Worker;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class WorkerCommand extends Command
{
    /**
     * @return void
     */
    protected function configure(): void
    {
        parent::configure();

        $this
            ->setName('worker')
            ->setDescription('Runs a Resque worker')
            ->addOption(
                'queue',
                'Q',
                InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY,
                'A queue to listen to and run jobs from'
            )
            ->addOption(
                'interval',
                'i',
                InputOption::VALUE_REQUIRED,
                'Interval in seconds to wait for between reserving jobs',
                5
            );
    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     * @return int
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $resque = $this->getResque($output);
        $queues = $input->getOption('queue');

        $worker = new Worker($resque, $queues);
        $worker->work((int)$input->getOption('interval'));

        return 0;
    }
}
