<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Transformer;

use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use WhiteDigital\EtlBundle\Helper\Queue;

#[AutoconfigureTag('etl.etl_transformer')]
interface TransformerInterface
{
    public function run(Queue $data): Queue;

    public function setOutput(OutputInterface $output): void;

    public function setOptions(array $options): void;
}
