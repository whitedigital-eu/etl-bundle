<?php

declare(strict_types = 1);

namespace WhiteDigital\EtlBundle\Helper;

use Doctrine\Common\Collections\ArrayCollection;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Statement;
use Doctrine\ORM\Mapping\ClassMetadata;
use Doctrine\ORM\Mapping\UnderscoreNamingStrategy;
use Doctrine\Persistence\ManagerRegistry;
use Symfony\Contracts\Service\Attribute\Required;
use WhiteDigital\EntityResourceMapper\Entity\BaseEntity;
use WhiteDigital\EntityResourceMapper\UTCDateTimeImmutable;
use WhiteDigital\EtlBundle\Exception\EtlException;

trait DbalHelperTrait
{
    private ManagerRegistry $doctrine;

    #[Required]
    public function setDoctrine(ManagerRegistry $doctrine): void
    {
        $this->doctrine = $doctrine;
    }

    /**
     * @param class-string $class
     */
    protected function getTableName(string $class): string
    {
        $tableName = $this->entityManager->getClassMetadata($class)->getTableName();
        if ('user' === $tableName) { // reserved keywords must be double-quoted in PostgreSQL
            $tableName = sprintf('"%s"', $tableName);
        }

        return $tableName;
    }

    /**
     * @param class-string $entity
     * @param string[]     $conditions
     */
    protected function createDBALDeleteQuery(string $entity, array $conditions): QueryBuilder
    {
        if (empty($conditions)) {
            throw new \RuntimeException('No conditions given DBAL for delete query');
        }
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();

        $queryBuilder->delete($this->getTableName($entity), 't');
        foreach ($conditions as $property => $value) {
            $column = $this->toColumnName($property);
            $queryBuilder
                ->andWhere(sprintf('t.%s = :%s', $column, $column))
                ->setParameter($column, $value);
        }

        return $queryBuilder;
    }

    protected function createDBALInsertQueryFromEntity(BaseEntity $entity, bool $addCreated = true): QueryBuilder
    {
        /**
         * @var Connection   $connection
         * @var QueryBuilder $query
         */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();

        $table = $this->getTableName($entity::class);

        /** @var ClassMetadata $entityMetaData */
        $entityMetaData = $this->doctrine->getManager()->getClassMetadata($entity::class);

        $queryBuilder->insert($table);

        $reflection = new \ReflectionObject($entity);
        $fieldNames = $entityMetaData->getFieldNames();
        $associationMappings = $entityMetaData->getAssociationMappings();

        foreach ($reflection->getProperties() as $property) {
            $propertyName = $property->getName();

            // process scalar types
            if (in_array($propertyName, $fieldNames, true) && (null !== $value = $property->getValue($entity))) {
                $columnName = $entityMetaData->getColumnName($propertyName);
                if ($value instanceof \DateTimeInterface) {
                    $value = $value->format(\DateTimeInterface::RFC3339);
                }
                if ($value instanceof \BackedEnum) {
                    $value = $value->value;
                }
                $type = ParameterType::STRING;
                if (is_bool($value)) {
                    $type = ParameterType::BOOLEAN;
                }
                $queryBuilder
                    ->setValue($columnName, ':' . $columnName)
                    ->setParameter($columnName, $value, $type);
                continue;
            }
            // process associations
            if (array_key_exists($propertyName, $associationMappings) && (null !== $value = $property->getValue($entity))) {
                if ($value instanceof ArrayCollection) {
                    // TODO how to proceed with many to many associations?
                    continue;
                }
                if (1 === count($joinColumn = $associationMappings[$propertyName]['joinColumnFieldNames'])) {
                    $columnName = current($joinColumn);
                    $queryBuilder
                        ->setValue($columnName, ':' . $columnName)
                        ->setParameter($columnName, $value->getId());
                    continue;
                }
                continue;
                // TODO handle multiple join columns
            }
        }

        if ($addCreated) {
            $queryBuilder->setValue('created_at', ':created_at')
                ->setParameter('created_at', (new UTCDateTimeImmutable())->format(\DateTimeInterface::RFC3339));
        }

        return $queryBuilder;
    }

    /**
     * Special case of update query, explicitly setting given fields to null.
     *
     * @param class-string $entity
     * @param string[]     $nullableFields
     */
    protected function createDBALUpdateQuerySetNull(string $entity, mixed $id, array $nullableFields): ?QueryBuilder
    {
        if (empty($nullableFields) || empty($id)) {
            return null;
        }
        /**
         * @var Connection   $connection
         * @var QueryBuilder $query
         */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();

        $queryBuilder->update($this->getTableName($entity));

        foreach ($nullableFields as $field) {
            $queryBuilder->set(sprintf('%s', $this->toColumnName($field)), 'NULL');
        }

        // Set condition
        $queryBuilder->where('id = :id')
            ->setParameter('id', $id);
        // Always set Updated
        $queryBuilder->set('updated_at', ':updated_at')
            ->setParameter('updated_at', (new UTCDateTimeImmutable())->format(\DateTimeInterface::RFC3339));

        return $queryBuilder;
    }

    /**
     * Will return null, if nothing to update, QueryBuilder otherwise.
     * ReplaceExisting=true will replace only scalar values for now.
     *
     * @param string[] $denyReplace
     *
     * @throws EtlException
     */
    protected function createDBALUpdateQueryFromEntity(BaseEntity $existingEntity, BaseEntity $newEntity, bool $replaceExisting = false, array $denyReplace = []): ?QueryBuilder
    {
        if ($existingEntity::class !== $newEntity::class) {
            throw new EtlException('createDBALUpdateQueryFromEntity must receive objects from same class.');
        }
        $hasChanges = false;
        /**
         * @var Connection   $connection
         * @var QueryBuilder $query
         */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();

        $table = $this->getTableName($existingEntity::class);

        /** @var ClassMetadata $entityMetaData */
        $entityMetaData = $this->doctrine->getManager()->getClassMetadata($existingEntity::class);

        $queryBuilder->update($table);

        $reflection = new \ReflectionObject($existingEntity);
        $fieldNames = $entityMetaData->getFieldNames();
        $associationMappings = $entityMetaData->getAssociationMappings();

        foreach ($reflection->getProperties() as $property) {
            $propertyName = $property->getName();
            if (in_array($propertyName, $denyReplace, true)) {
                continue;
            }

            // process scalar and datetime types
            if (in_array($propertyName, $fieldNames, true)
                && ((null !== $value = $property->getValue($newEntity)) && (!empty($value))) // new value is not NULL nor empty array
                && (null === $property->getValue($existingEntity) || ($replaceExisting && !$this->isEqual($property->getValue($existingEntity), $property->getValue($newEntity)))) // existing value is NULL OR we allow to replace existing value explicitly
            ) {
                $columnName = $entityMetaData->getColumnName($propertyName);
                if ($value instanceof \DateTimeInterface) {
                    $value = $value->format(\DateTimeInterface::RFC3339);
                }
                if ($value instanceof \BackedEnum) {
                    $value = $value->value;
                }
                $type = ParameterType::STRING;
                if (is_bool($value)) {
                    $type = ParameterType::BOOLEAN;
                }
                $queryBuilder
                    ->set($columnName, ':' . $columnName)
                    ->setParameter($columnName, $value, $type);
                $hasChanges = true;
                continue;
            }

            // process associations
            $value = $property->getValue($newEntity);
            if ($value instanceof ArrayCollection) {
                // TODO how to proceed with many to many associations? Skip for now.
                continue;
            }
            if (array_key_exists($propertyName, $associationMappings)
                && (null !== $value)
                && (null === $property->getValue($existingEntity) || ($replaceExisting && (!$this->isEqual($property->getValue($existingEntity)->getId(), $value->getId()))))
            ) {

                if (1 === count($joinColumn = $associationMappings[$propertyName]['joinColumnFieldNames'])) {
                    $columnName = current($joinColumn);
                    $queryBuilder
                        ->set($columnName, ':' . $columnName)
                        ->setParameter($columnName, $value->getId());
                    $hasChanges = true;
                    continue;
                }
                continue;
                // TODO handle multiple join columns
            }
        }

        if (!$hasChanges) {
            return null;
        }

        // set condition
        $queryBuilder->where('id = :id')
            ->setParameter('id', $existingEntity->getId());
        // always set Updated
        $queryBuilder->set('updated_at', ':updated_at')
            ->setParameter('updated_at', (new UTCDateTimeImmutable())->format(\DateTimeInterface::RFC3339));

        return $queryBuilder;
    }

    /**
     * @throws Exception
     */
    protected function selectIdByValue(string $table, array $param): ?int
    {
        $keys = array_keys($param);
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();
        $res = $queryBuilder->select('id')
            ->from($table)
            ->where(sprintf('%s = ?', $this->toColumnName($keys[0])))
            ->setParameter(0, $param[$keys[0]])
            ->executeQuery();

        return $res->fetchAssociative()['id'] ?? null;
    }

    /**
     * Create `INSERT INTO <table> (a, b, c) VALUES(:a, :b, :c) RETURNING id` Statement object
     * which can be then used to insert related entities.
     *
     * @throws Exception
     */
    protected function createDBALInsertQueryReturning(string $table, array|object $data, bool $addCreated = true): Statement
    {
        $data = $this->normalizeData($data);
        if ($addCreated) {
            $data['created_at'] = (new UTCDateTimeImmutable())->format(\DateTimeInterface::RFC3339);
        }
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();
        $mainQuery = sprintf('insert into %s (%s) values (%s) returning id', $table, $this->traverseKeys($data), $this->traverseKeys($data, true));
        $statement = $connection->prepare($mainQuery);
        foreach ($data as $key => $value) {
            $statement->bindValue($this->toColumnName($key), $value);
        }

        return $statement;
    }

    /**
     * Execute `INSERT INTO <table> (a, b, c) VALUES(:a, :b, :c) RETURNING id` Statement object
     * which can be then used to insert related entities.
     *
     * @throws Exception
     */
    protected function executeDBALInsertQueryReturning(string $table, array|object $data, bool $addCreated = true): int
    {
        $data = $this->normalizeData($data);
        if ($addCreated) {
            $data['created_at'] = (new UTCDateTimeImmutable())->format(\DateTimeInterface::RFC3339);
        }
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();
        $query = sprintf('insert into %s (%s) values (%s) returning id', $table, $this->traverseKeys($data), $this->traverseKeys($data, true));
        $result = $connection->executeQuery($query, $data);

        return $result->fetchAssociative()['id'];
    }

    /**
     * Will return array where first field will be key and rest as array.
     *
     * @return array<string, array>
     *
     * @throws Exception
     */
    protected function createExistingHashMap(string $sql): array
    {
        $output = [];

        /**
         * @var Connection $connection
         */
        $connection = $this->doctrine->getConnection();
        $results = $connection->executeQuery($sql);
        foreach ($results->iterateAssociative() as $record) {
            $isFirst = true;
            $hash = '';
            foreach ($record as $key => $value) {
                if ($isFirst) {
                    $hash = $value;
                    $isFirst = false;
                    continue;
                }
                $output[$hash][$key] = $value;
            }
        }

        return $output;
    }

    /**
     * Convert associative array to string list for use in SQL queries or named parameters.
     */
    private function traverseKeys(array $data, bool $asParams = false): string
    {
        $outputArray = [];
        foreach ($data as $key => $value) {
            if (null === $value) {
                continue;
            }
            $outputArray[] = $asParams ? sprintf(':%s', $this->toColumnName($key)) : $this->toColumnName($key);
        }

        return implode(',', $outputArray);
    }

    /**
     * Normalize stdClass (or other simple) object to array.
     */
    private function normalizeData(array|object $data): array
    {
        if (!is_array($data)) {
            return get_object_vars($data);
        }

        return $data;
    }

    private function toColumnName(string $propertyName): string
    {
        return (new UnderscoreNamingStrategy())->propertyToColumnName($propertyName);
    }

    /**
     * Return true, if two scalar values are equal or two DateTime objects represents same time.
     */
    private function isEqual(mixed $a, mixed $b): bool
    {
        if ($this->isFloat($a) && $this->isFloat($b)) {
            return abs((float)$a - (float)$b) < 0.0001;
        }

        if ($a instanceof \DateTimeInterface && $b instanceof \DateTimeInterface) {
            return $a->getTimestamp() === $b->getTimestamp();
        }

        return $a === $b;
    }

    private function isFloat($str): bool
    {
        return filter_var($str, FILTER_VALIDATE_FLOAT) !== false;
    }
}
