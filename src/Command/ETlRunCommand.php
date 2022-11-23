<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Command;

use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use WhiteDigital\EtlBundle\Task\EtlTaskRunner;

#[AsCommand('etl:run')]
class ETlRunCommand extends Command
{
    private const ETL_PIPELINE_ID = 'etl_pipeline_id';

    public function __construct(
        private readonly EtlTaskRunner $etlTaskRunner,
    )
    {
        parent::__construct();
    }

    /**
     * @throws \ReflectionException
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $output->writeln('<info>ETL task runner started. You can see available tasks by etl:list command.</info>');
        $this->etlTaskRunner->runTaskByName($output, name: $input->getArgument(self::ETL_PIPELINE_ID));
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
