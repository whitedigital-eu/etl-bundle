<?php declare(strict_types = 1);

namespace WhiteDigital\EtlBundle\Helper;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use WhiteDigital\EntityResourceMapper\UTCDateTimeImmutable;
use function Symfony\Component\String\u;

class TemporaryTable
{
    public function __construct(
        public readonly Connection $connection,
    ) {
    }

    /**
     * @throws Exception|\ReflectionException
     */
    public function create(string $name, string $tempTableClass, bool $createRealTable = false): void
    {
        if (!in_array(TemporaryTableInterface::class, class_implements($tempTableClass), true)) {
            throw new Exception('Class must implement TempTableInterface');
        }
        if (!$createRealTable) {
            $template = 'CREATE TEMPORARY TABLE %s(%s)';
        } else {
            $template = 'CREATE TABLE %s(%s)';
        }

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
            if ($value instanceof \DateTimeInterface) {
                $value = $value->format('Y-m-d H:i:s');
            }
            $columns[] = $this->toSnakeCase($column);
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
     * @throws Exception
     */
    public function queryOne(string $sql): ?array
    {
        $result = $this->connection->fetchAllAssociative($sql);
        if (0 === count($result)) {
            return null;
        }

        if (1 !== count($result)) {
            throw new \RuntimeException('Expected one row, got ' . count($result));
        }

        return $result[0];
    }

    /**
     * @template T of TemporaryTableInterface
     *
     * @param class-string<T> $tempTableClass
     *
     * @return T|null
     *
     * @throws Exception
     * @throws \ReflectionException
     */
    public function queryOneToTempTable(string $sql, string $tempTableClass): ?TemporaryTableInterface
    {
        if (!in_array(TemporaryTableInterface::class, class_implements($tempTableClass), true)) {
            throw new Exception('Class must implement TempTableInterface');
        }
        $result = $this->queryOne($sql);
        if (null === $result) {
            return null;
        }
        $tempTable = new $tempTableClass();
        $classReflection = new \ReflectionClass($tempTableClass);
        foreach ($result as $column => $value) {
            $reflectionType = $classReflection->getProperty($this->toCamelCase($column))->getType();
            if ($reflectionType instanceof \ReflectionNamedType) {
                $value = match ($reflectionType->getName()) {
                    'int' => (int) $value,
                    'float' => (float) $value,
                    'bool' => (bool) $value,
                    default => $value,
                };
            } else {
                throw new \RuntimeException('Temp table contains unnamed type property');
            }

            $tempTable->{$this->toCamelCase($column)} = $value;
        }

        return $tempTable;
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
            $columns[] = $this->toSnakeCase($property->getName()) . ' ' . $this->getDbType($property->getType());
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
            UTCDateTimeImmutable::class => 'timestamp',
            default => throw new \RuntimeException("Unsupported type {$type->getName()} for temporary table."),
        };
    }

    /**
     * Converts fooBar to foo_bar - PG does not support camelCase column names
     * @param string $input
     * @return string
     */
    private function toSnakeCase(string $input): string
    {
        return (string) u($input)->snake();
    }

    /**
     * Converts foo_bar to fooBar - PG does not support camelCase column names
     * @param string $input
     * @return string
     */
    private function toCamelCase(string $input): string
    {
        return (string) u($input)->camel();
    }
}
