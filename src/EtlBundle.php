<?php

/**
 * @author andis @ 22.11.2022
 */

declare(strict_types=1);

namespace WhiteDigital\EtlBundle;

use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Bundle\AbstractBundle;

class EtlBundle extends AbstractBundle
{

 public function loadExtension(array $config, ContainerConfigurator $container, ContainerBuilder $builder): void
 {
     $container->import('../config/services.php');
 }
}
