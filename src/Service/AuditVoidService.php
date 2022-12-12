<?php declare(strict_types = 1);

namespace WhiteDigital\EtlBundle\Service;

use Throwable;
use WhiteDigital\Audit\Contracts\AuditServiceInterface;

class AuditVoidService implements AuditServiceInterface
{
    public function audit(string $type, string $message, array $data = []): void
    {
    }

    public function auditException(Throwable $exception, ?string $url = null): void
    {
    }
}
