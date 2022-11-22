<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

class EtlValidator
{
    public function __construct(
        private ValidatorType $type,
        private string $description,
        private \Closure $validator,
    ) {
    }

    public function run(mixed $data): bool
    {
        return $this->validator->__invoke($data);
    }

    public function getDescription(): string
    {
        return $this->description;
    }

    public function getType(): ValidatorType
    {
        return $this->type;
    }
}
