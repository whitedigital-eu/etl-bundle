<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Attribute;

/**
 * Service tag to autoconfigure tasks.
 */
#[\Attribute(\Attribute::TARGET_CLASS)]
class AsTask
{
    public function __construct(
        public string $name,
    )
    {
    }
}
