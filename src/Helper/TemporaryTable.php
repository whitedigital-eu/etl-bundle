<?php

declare(strict_types = 1);

namespace WhiteDigital\EtlBundle\Helper;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;

class TemporaryTable
{
    public function __construct(
        public readonly Connection $connection,
    ) {
    }

    /**
     * @throws Exception|\ReflectionException
     */
    public function create(string $name, string $tempTableClass): void
    {
        if (!in_array(TemporaryTableInterface::class, class_implements($tempTableClass), true)) {
            throw new Exception('Class must implement TempTableInterface');
        }
        $template = 'CREATE TEMPORARY TABLE %s(%s)';

        $this->connection->executeQuery(sprintf($template, $name, $this->getDbColumns($tempTableClass)));
    }

    /**
     * @throws Exception
     */
    public function insert(string $tableName, TemporaryTableInterface $data): void
    {
        $columns = [];
        $values = [];
        foreach (get_object_vars($data) as $column => $value) {
            if (null === $value) {
                continue;
            }
            $columns[] = $column;
            $values[] = $value;
        }

        $this->connection->insert($tableName, array_combine($columns, $values));
    }

    /**
     * @return array<int, array<string, mixed>>
     *
     * @throws Exception
     */
    public function query(string $sql): array
    {
        return $this->connection->fetchAllAssociative($sql);
    }

    /**
     * @return string[]
     *
     * @throws \ReflectionException
     */
    public function getColumns(string $tempTableClass): array
    {
        $columns = [];
        $reflection = new \ReflectionClass($tempTableClass);
        foreach ($reflection->getProperties() as $property) {
            $columns[] = $property->getName();
        }

        return $columns;
    }

    /**
     * @throws \ReflectionException
     */
    private function getDbColumns(string $tempTableClass): string
    {
        $columns = [];
        $reflection = new \ReflectionClass($tempTableClass);
        foreach ($reflection->getProperties() as $property) {
            $columns[] = $property->getName() . ' ' . $this->getDbType($property->getType());
        }

        return implode(', ', $columns);
    }

    private function getDbType(?\ReflectionType $type): string
    {
        if (null === $type) {
            throw new \RuntimeException('Type is missing for temporary table column');
        }

        if (!$type instanceof \ReflectionNamedType) {
            throw new \RuntimeException('Type is not named type for temporary table column');
        }

        return match ($type->getName()) {
            'int' => 'INT',
            'string' => 'VARCHAR',
            'float' => 'FLOAT',
            'bool' => 'BOOLEAN',
            default => throw new \RuntimeException("Unsupported type {$type->getName()} for temporary table column"),
        };
    }
}
