<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

use DateTimeInterface;
use Doctrine\Common\Collections\ArrayCollection;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Statement;
use Doctrine\ORM\Mapping\ClassMetadata;
use Doctrine\ORM\Mapping\UnderscoreNamingStrategy;
use Doctrine\Persistence\ManagerRegistry;
use RuntimeException;
use JetBrains\PhpStorm\ArrayShape;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Contracts\Service\Attribute\Required;
use WhiteDigital\EntityResourceMapper\Entity\BaseEntity;
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
     * @deprecated
     * @throws Exception
     */
    protected function executeDBALInsertQuery(string $table, array|object $data, bool $addCreated = true): void
    {
        $query = $this->createDBALInsertQuery($table, $data, $addCreated);
        $query->executeQuery();
    }

    /**
     * @deprecated
     * @throws EtlException
     * @throws Exception
     */
    protected function executeDBALUpdateQuery(string $table, array|object $data, int $id): void
    {
        $query = $this->createDBALUpdateQuery($table, $data, $id);
        $query->executeQuery();
    }

    /**
     * @deprecated
     */
    protected function createDBALInsertQuery(string $table, array|object $data, bool $addCreated = true): QueryBuilder
    {
        /**
         * @var Connection   $connection
         * @var QueryBuilder $query
         */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();
        $queryBuilder->insert($table);
        $data = $this->normalizeData($data);
        foreach ($data as $col => $val) {
            $queryBuilder
                ->setParameter($this->toColumnName($col), $val)
                ->setValue($this->toColumnName($col), ':'.$this->toColumnName($col));
        }
        if ($addCreated) {
            $queryBuilder->setValue('created_at', ':created_at')
                ->setParameter('created_at', (new \DateTime())->format('Y-m-d H:i:s'));
        }

        return $queryBuilder;
    }

    /**
     * @param class-string $entity
     * @param string[] $conditions
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
                if ($value instanceof DateTimeInterface) {
                    $value = $value->format(DateTimeInterface::RFC3339);
                }
                if ($value instanceof \BackedEnum) {
                    $value = $value->value;
                }
                $queryBuilder
                    ->setValue($columnName, ':'.$columnName)
                    ->setParameter($columnName, $value);
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
                        ->setValue($columnName, ':'.$columnName)
                        ->setParameter($columnName, $value->getId());
                    continue;
                } else {
                    continue;
                    // TODO handle multiple join columns
                }
            }
        }

        if ($addCreated) {
            $queryBuilder->setValue('created_at', ':created_at')
                ->setParameter('created_at', (new \DateTime())->format(DateTimeInterface::RFC3339));
        }

        return $queryBuilder;
    }

    /**
     * Special case of update query, explicitly setting given fields to null.
     * @param class-string $entity
     * @param string[] $nullableFields
     * @return QueryBuilder
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
            ->setParameter('updated_at', (new \DateTimeImmutable())->format(DateTimeInterface::RFC3339));

        return $queryBuilder;

    }


    /**
     * Will return null, if nothing to update, QueryBuilder otherwise.
     * ReplaceExisting=true will replace only scalar values for now.
     *
     * @throws EtlException
     */
    protected function createDBALUpdateQueryFromEntity(BaseEntity $existingEntity, BaseEntity $newEntity, bool $replaceExisting = false): ?QueryBuilder
    {
        if (get_class($existingEntity) !== get_class($newEntity)) {
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

            // process scalar types
            if (in_array($propertyName, $fieldNames, true)
                && (null !== $value = $property->getValue($newEntity)) // new value is not NULL
                && (null === $property->getValue($existingEntity) || ($replaceExisting && $property->getValue($existingEntity) !== $property->getValue($newEntity))) // existing value is NULL OR we allow to replace existing value explicitly
            ) {
                $columnName = $entityMetaData->getColumnName($propertyName);
                if ($value instanceof DateTimeInterface) {
                    $value = $value->format(DateTimeInterface::RFC3339);
                }
                if ($value instanceof \BackedEnum) {
                    $value = $value->value;
                }
                $queryBuilder
                    ->set($columnName, ':'.$columnName)
                    ->setParameter($columnName, $value);
                $hasChanges = true;
                continue;
            }
            // process associations
            if (array_key_exists($propertyName, $associationMappings)
                && (null === $property->getValue($existingEntity))
                && (null !== $value = $property->getValue($newEntity))
            ) {
                if ($value instanceof ArrayCollection) {
                    // TODO how to proceed with many to many associations?
                    continue;
                }
                if (1 === count($joinColumn = $associationMappings[$propertyName]['joinColumnFieldNames'])) {
                    $columnName = current($joinColumn);
                    $queryBuilder
                        ->set($columnName, ':'.$columnName)
                        ->setParameter($columnName, $value->getId());
                    $hasChanges = true;
                    continue;
                } else {
                    continue;
                    // TODO handle multiple join columns
                }
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
            ->setParameter('updated_at', (new \DateTimeImmutable())->format(DateTimeInterface::RFC3339));

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
     * @throws Exception
     */
    protected function executeDBALInsertIfNotExistQuery(string $table, array|object $data, bool $addCreated = true): bool
    {
        $conditions = [];
        $data = $this->normalizeData($data);
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();

        foreach ($data as $key => $value) {
            $conditions[] = sprintf('%s = :%s', $this->toColumnName($key), $key);
        }
        $condition = implode(' AND ', $conditions);
        $validate = sprintf('SELECT * FROM %s WHERE %s', $table, $condition);
        $results = $connection->executeQuery($validate, $data);
        if (0 === $results->rowCount()) {
            $q = $this->createDBALInsertQuery($table, $data, $addCreated);
            $q->executeQuery();

            return true;
        }

        return false;
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
            $data['created_at'] = (new \DateTime())->format(DateTimeInterface::RFC3339);
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
            $data['created_at'] = (new \DateTime())->format(DateTimeInterface::RFC3339);
        }
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();
        $query = sprintf('insert into %s (%s) values (%s) returning id', $table, $this->traverseKeys($data), $this->traverseKeys($data, true));
        $result = $connection->executeQuery($query, $data);

        return $result->fetchAssociative()['id'];
    }

    /**
     * @deprecated
     */
    protected function createDBALUpdateQuery(string $table, array $data, int $id): ?QueryBuilder
    {
        if (empty($data)) {
            return null;
        }
        if (empty($id)) {
            throw new EtlException(sprintf('empty ID received for %s', __METHOD__));
        }
        /**
         * @var Connection $connection ;
         * @var QueryBuilder $query
         */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();
        $queryBuilder->update($table);
        foreach ($data as $col => $val) {
            $queryBuilder->set($this->toColumnName($col), ':' . $this->toColumnName($col))
                ->setParameter($this->toColumnName($col), $val);
        }
        // set condition
        $queryBuilder->where('id = :id')
            ->setParameter('id', $id);
        // always set Updated
        $queryBuilder->set('updated_at', ':updated_at')
            ->setParameter('updated_at', (new DateTime())->format('Y-m-d H:i:s'));

        return $queryBuilder;
    }

    /**
     * In case when child records needs to be inserted with ID created in parent record insert.
     * Return array shape: ['main' => <Statement>, 'child' => <QueryBuilder[]> ].
     *
     * @throws Exception
     */
    #[ArrayShape(['main' => Statement::class, 'child' => 'array'])]
    protected function createDBALChainedInsertQuery(string $mainTable, array|object $mainData, array $childTableData, bool $addCreated = true): array
    {
        $output = [
            'main' => $this->createDBALInsertQueryReturning($mainTable, $mainData, $addCreated),
            'child' => [],
        ];
        foreach ($childTableData as $tableName => $data) {
            $output['child'][] = $this->createDBALInsertQuery($tableName, $data);
        }

        return $output;
    }

    /**
     * @deprecated
     */
    protected function returnUpdatedFields(BaseEntity $existingEntity, object $transformedRecord, array $skipFields = [], bool $replaceExisting = false): array
    {
        $propertyAccessor = PropertyAccess::createPropertyAccessor();
        $updatedFields = [];
        foreach ($transformedRecord as $key => $value) {
            if (in_array($key, $skipFields, true)) {
                continue;
            }
            $isRelation = false;
            if (str_ends_with($key, 'Id')) { // It is ORM relation. For example customerId, we should get customer->getId()
                $isRelation = true;
                $relationName = substr($key, 0, -2);
                $getterMethod = 'get' . ucfirst($relationName);
                if (!method_exists($existingEntity, $getterMethod)) {
                    throw new RuntimeException("{$getterMethod} does not exist in existingEntity object.");
                }
                try {
                    $propertyValue = $existingEntity->{$getterMethod}()?->getId();
                } catch (\Throwable $error) {
                    $propertyValue = null;
                }
            } else {
                $propertyValue = $propertyAccessor->getValue($existingEntity, $key);
            }

            // Update only NULL values or any changed value if $replaceExisting === true
            if (null !== $value && ((null === $propertyValue && false === $replaceExisting) || ($value !== $propertyValue && true === $replaceExisting && !is_object($propertyValue)))) {
                $updatedFields[$key] = $value;
                // Let's set value for existing object, so repeating updates are skipped in current batch
                if ($isRelation) {
                    continue; // TODO  if property is relation, won't update it for now.
                }
                $reflectionProperty = new \ReflectionProperty($existingEntity, $key);
                if (\DateTimeInterface::class === $reflectionProperty->getType()?->getName()) {
                    $value = new \DateTimeImmutable($value);
                }
                $propertyAccessor->setValue($existingEntity, $key, $value);
            }
        }

        return $updatedFields;
    }

    /**
     * Will return array where first field will be key and rest as array
     * @param string $sql
     * @return array<string, array>
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
            foreach($record as $key => $value) {
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


}
