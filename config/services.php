<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace Symfony\Component\DependencyInjection\Loader\Configurator;


return static function (ContainerConfigurator $container) {
    $container->services()->load('WhiteDigital\\EtlBundle\\','../src/')
        ->autoconfigure()
        ->autowire();
};
