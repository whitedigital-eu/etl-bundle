<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Task;

use WhiteDigital\EtlBundle\EtlPipeline;

abstract class AbstractTask implements EtlTaskInterface
{
    public string $taskName;
    public int $taskId;

    public function __construct(
        protected readonly EtlPipeline $etlPipeline,
    )
    {
    }
}
