<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Transformer;

use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use WhiteDigital\EtlBundle\Helper\Queue;

#[AutoconfigureTag('etl.transformer')]
interface TransformerInterface
{
    /**
     * @template T
     * @param Queue<T> $data
     * @return Queue<T>
     */
    public function run(Queue $data): Queue;

    public function setOutput(OutputInterface $output): void;

    /**
     * @param array<string, mixed> $options
     * @return void
     */
    public function setOptions(array $options): void;
    public function getOption(string $key): mixed;
    public function displayStartupMessage(): void;
    public function printValidatorFailures(): void;
}
