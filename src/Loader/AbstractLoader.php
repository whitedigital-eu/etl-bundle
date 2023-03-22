<?php declare(strict_types = 1);

/**
 * @author andis @ 23.11.2022
 */

namespace WhiteDigital\EtlBundle\Loader;

use Symfony\Component\Console\Output\OutputInterface;

abstract class AbstractLoader implements LoaderInterface
{
    protected OutputInterface $output;

    /** @var array<string, mixed> */
    private array $options;

    public function setOutput(OutputInterface $output): void
    {
        $this->output = $output;
    }

    /**
     * @param string[] $options
     */
    public function setOptions(array $options): void
    {
        $this->options = $options;
    }

    public function getOption(string $key): mixed
    {
        return $this->options[$key] ?? null;
    }

    public function displayStartupMessage(): void
    {
        $this->output->writeln(sprintf("\n<info>%s</info> uzsākts\n", static::class));
    }
}
