<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Task;

use WhiteDigital\EtlBundle\Attribute\AsTask;
use WhiteDigital\EtlBundle\EtlPipeline;
use WhiteDigital\EtlBundle\Exception\EtlException;

abstract class AbstractTask implements EtlTaskInterface
{
    /**
     * @throws EtlException
     */
    public function __construct(
        protected readonly EtlPipeline $etlPipeline,
    )
    {
        $this->etlPipeline->setPipelineId($this->getTaskName());
    }

    /**
     * @throws EtlException
     */
    protected function getTaskName(): string
    {
        $asTaskAttributes = (new \ReflectionClass($this))->getAttributes(AsTask::class);
        if (1 !== count($asTaskAttributes)) {
            throw new EtlException(sprintf('Task %s must have AsTask() attribute defined.', get_class($this)));
        }
        return $asTaskAttributes[0]->getArguments()['name'];
    }


}
