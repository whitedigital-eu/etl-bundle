<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Mailer\Exception\TransportExceptionInterface;
use WhiteDigital\EtlBundle\Task\EtlTasks;

#[AsCommand('etl:run')]
class ETlRunCommand extends Command
{
    private const ETL_PIPELINE_ID = 'etl_pipeline_id';

    public function __construct(
        private readonly EtlTasks $etlTasks,
    )
    {
        parent::__construct();
    }

    /**
     * @throws TransportExceptionInterface
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $output->writeln('ETL task runner started.');
        $this->etlTasks->runTaskById($input->getArgument(self::ETL_PIPELINE_ID), $output);
        return Command::SUCCESS;
    }

    protected function configure(): void
    {
        $this
            ->addArgument(self::ETL_PIPELINE_ID, InputArgument::REQUIRED, 'ID of ETL Pipeline')
            ->setDescription('ETL pipeline runner')
            ->setHelp('Runs predefined Extract/Transform/Load pipelines');
    }

}
