<?php

/**
 * @author andis @ 23.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Extractor;

use Symfony\Component\Console\Output\OutputInterface;
use WhiteDigital\EtlBundle\Exception\ExtractorException;

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

    /**
     * @throws ExtractorException
     */
    public function getOption(string $key): mixed
    {
        if (!array_key_exists($key, $this->options)) {
            throw new ExtractorException(sprintf('Requested option key [%s] not set', $key));
        }

        return $this->options[$key];
    }

    public function displayStartupMessage(): void
    {
        $this->output->writeln(sprintf("\n<info>%s</info> uzsākts\n", get_class($this)));
    }

}
