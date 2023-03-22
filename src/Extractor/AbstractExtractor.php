<?php

/**
 * @author andis @ 23.11.2022
 */

declare(strict_types = 1);

namespace WhiteDigital\EtlBundle\Extractor;

use Symfony\Component\Console\Output\OutputInterface;

abstract class AbstractExtractor implements ExtractorInterface
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
