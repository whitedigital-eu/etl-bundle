<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

use Doctrine\ORM\EntityManagerInterface;

class RepositoryCacheService
{
    private array $_cache;

    public function __construct(
        private EntityManagerInterface $entityManager,
    ) {
    }

    /**
     * Explicitly fetch lazy, except for $fetchEager fields.
     */
    public function storeExistingRecords(string $class, array $fetchEager, string $indexProperty): void
    {
        $repository = $this->entityManager->getRepository($class);
        $indexedCollection = $repository->findAllLazy($fetchEager, $indexProperty);
        $this->entityManager->clear();
        $this->_cache[$class] = $indexedCollection;
    }

    public function getExistingRecords(string $class): array
    {
        return $this->_cache[$class];
    }
}
