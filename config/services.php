<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace Symfony\Component\DependencyInjection\Loader\Configurator;


use WhiteDigital\EtlBundle\EtlPipeline;

return static function (ContainerConfigurator $container) {
    $container->services()->load('WhiteDigital\\EtlBundle\\','../src/')
        ->autoconfigure()
        ->autowire();

    $container->services() // ETL pipeline service must be instantiated separately for each task
        ->set(EtlPipeline::class)
        ->autowire()
        ->autoconfigure()
        ->share(false);
};
