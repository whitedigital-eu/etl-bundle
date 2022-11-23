<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Loader;

use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;
use WhiteDigital\EtlBundle\Helper\Queue;

#[AutoconfigureTag('etl.loader')]
interface LoaderInterface
{
    public function run(Queue $data): void;

    public function setOutput(OutputInterface $output): void;

    public function setOptions(array $options): void;

    public function displayStartupMessage(): void;
}
