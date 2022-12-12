<?php

declare(strict_types = 1);

namespace Symfony\Component\DependencyInjection\Loader\Configurator;

use WhiteDigital\Audit\Contracts\AuditServiceInterface;
use WhiteDigital\EtlBundle\Service\AuditVoidService;

return static function (ContainerConfigurator $container): void {
    $container
        ->services()
            ->set(AuditServiceInterface::class)
            ->class(AuditVoidService::class);
};
