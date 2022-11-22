<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Task;

use Symfony\Component\DependencyInjection\Attribute\TaggedLocator;
use Symfony\Component\DependencyInjection\ServiceLocator;

class EtlTaskRunner
{
    private ServiceLocator $tasks;


    public function __construct(
        #[TaggedLocator(tag: 'etl.task')] private ServiceLocator   $extractors,
    ) {
    }
    public function runTaskById(): void
    {
        

    }

}
