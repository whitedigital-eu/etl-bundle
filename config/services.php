<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace Symfony\Component\DependencyInjection\Loader\Configurator;

use WhiteDigital\EtlBundle\Service\DummyService;

return static function (ContainerConfigurator $container) {
// $container->services()->load('WhiteDigital\EtlBundle\\','../src');
    $container->services()->set(DummyService::class);
};
