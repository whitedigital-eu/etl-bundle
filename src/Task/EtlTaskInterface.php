<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Task;

use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\AutoconfigureTag;

#[AutoconfigureTag('etl.task')]
interface EtlTaskInterface
{
    public function runTask(OutputInterface $output): void;
}
