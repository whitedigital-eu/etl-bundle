<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Extractor;

use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use WhiteDigital\EtlBundle\Helper\Queue;

#[AutoconfigureTag('etl.etl_extractor')]
interface ExtractorInterface
{
    /**
     * @template T
     * @param \Closure|null $batchProcessor
     * @return Queue<T>|null
     */
    public function run(\Closure $batchProcessor = null): Queue|null;

    public function setOutput(OutputInterface $output): void;

    public function setOptions(array $options): void;
}
