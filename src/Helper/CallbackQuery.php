<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

class CallbackQuery
{
    public function __construct(
        private readonly \Closure $callbackFunction
    ) {
    }

    /**
     * Execute callback function and return stats (number of inserts and updates).
     * @return array{insert: int|mixed, update: int|mixed, delete: int|mixed, log: array}
     */
    public function execute(): array
    {
        $result = $this->callbackFunction->__invoke();
        $inserts = $result['insert'] ?? 0;
        $updates = $result['update'] ?? 0;
        $deletes = $result['delete'] ?? 0;
        $log = $result['log'] ?? 0;

        return [
            'insert' => $inserts,
            'update' => $updates,
            'delete' => $deletes,
            'log' => $log,
        ];
    }
}
