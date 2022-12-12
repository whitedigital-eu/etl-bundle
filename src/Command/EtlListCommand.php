<?php

/**
 * @author andis @ 23.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\TaggedLocator;
use Symfony\Component\DependencyInjection\ServiceLocator;
use WhiteDigital\EtlBundle\Attribute\AsTask;

#[AsCommand(name: 'etl:list')]
class EtlListCommand extends Command
{
    public function __construct(
        #[TaggedLocator(tag: 'etl.task')] private readonly ServiceLocator $tasks,
    ) {
        parent::__construct();
    }

    /**
     * @throws \ReflectionException
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $output->writeln('<info>List of available ETL Tasks</info>');

        $table = (new Table($output))
            ->setHeaders(['#', 'Name', 'FQCN']);

        $ix = 1;
        /** @var class-string $task */
        foreach ($this->tasks->getProvidedServices() as $task) {
            $reflection = new \ReflectionClass($task);
            $asTaskAttributes = $reflection->getAttributes(AsTask::class);
            if (1 !== count($asTaskAttributes)) {
                throw new \RuntimeException('Single AsTask() attribute must be set for class '.$task);
            }
            $asTaskAttribute = $reflection->getAttributes(AsTask::class)[0];
            $table->addRow([$ix++, $asTaskAttribute->getArguments()['name'], $task]);
        }

        $table->render();

        return Command::SUCCESS;
    }

    protected function configure(): void
    {
        $this
            ->setDescription('List available ETL Tasks');
    }
}
