<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

use App\ETL\Exception\EtlException;
use DateTime;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\DBAL\Statement;
use Doctrine\ORM\Mapping\UnderscoreNamingStrategy;
use Doctrine\Persistence\ManagerRegistry;
use http\Exception\RuntimeException;
use JetBrains\PhpStorm\ArrayShape;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Contracts\Service\Attribute\Required;
use WhiteDigital\EntityResourceMapper\Entity\BaseEntity;

trait DbalHelperTrait
{
    private ManagerRegistry $doctrine;

    #[Required]
    public function setDoctrine(ManagerRegistry $doctrine): void
    {
        $this->doctrine = $doctrine;
    }

    /**
     * @throws Exception
     */
    protected function executeDBALInsertQuery(string $table, array|object $data, bool $addCreated = true): void
    {
        $query = $this->createDBALInsertQuery($table, $data, $addCreated);
        $query->executeQuery();
    }

    /**
     * @param bool $addCreated
     *
     * @throws EtlException
     * @throws Exception
     */
    protected function executeDBALUpdateQuery(string $table, array|object $data, int $id): void
    {
        $query = $this->createDBALUpdateQuery($table, $data, $id);
        $query->executeQuery();
    }

    protected function createDBALInsertQuery(string $table, array|object $data, bool $addCreated = true): QueryBuilder
    {
        /**
         * @var Connection   $connection ;
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
                ->setParameter('created_at', (new DateTime())->format('Y-m-d H:i:s'));
        }

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
            $data['createdAt'] = (new \DateTime())->format('Y-m-d H:i:s');
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
            $data['createdAt'] = (new \DateTime())->format('Y-m-d H:i:s');
        }
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();
        $query = sprintf('insert into %s (%s) values (%s) returning id', $table, $this->traverseKeys($data), $this->traverseKeys($data, true));
        $result = $connection->executeQuery($query, $data);

        return $result->fetchAssociative()['id'];
    }

    /**
     * @throws EtlException
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
         * @var Connection   $connection ;
         * @var QueryBuilder $query
         */
        $connection = $this->doctrine->getConnection();
        $queryBuilder = $connection->createQueryBuilder();
        $queryBuilder->update($table);
        foreach ($data as $col => $val) {
            $queryBuilder->set($this->toColumnName($col), ':'.$this->toColumnName($col))
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
     * @throws \ReflectionException
     * @throws \Exception
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
                $getterMethod = 'get'.ucfirst($relationName);
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
