<?php declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Task;

use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\TaggedLocator;
use Symfony\Component\DependencyInjection\ServiceLocator;
use WhiteDigital\EtlBundle\Attribute\AsTask;

class EtlTaskRunner
{
    /** @var string[]  */
    private ?array $customArgs = null;

    public function __construct(
        #[TaggedLocator(tag: 'etl.task')] private readonly ServiceLocator $tasks,
    )
    {
    }

    /**
     * @throws \ReflectionException
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function runTaskByName(OutputInterface $output, string $name): void
    {
        foreach ($this->tasks->getProvidedServices() as $task) {
            $asTaskAttribute = (new \ReflectionClass($task))
                ->getAttributes(AsTask::class)[0];
            $taskArguments = $asTaskAttribute->getArguments();
            if ($name === $taskArguments['name']) {
                /** @var EtlTaskInterface $etlTask */
                $etlTask = $this->tasks->get($task);
                $etlTask->runTask($output, $this->customArgs);
                return;
            }
        }
        $output->writeln(sprintf('Task {name: %s} not found.', $name));
    }

    public function addCustomArg(string $name, string $value): void
    {
        $this->customArgs[$name] = $value;
    }
}
