<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

/**
 * Simple Queue (FIFO) implementation based on PHP \Array structure.
 *
 * @template T
 */
class Queue implements \Countable
{
    /**
     * @var array<T> $__queue
     */
    private array $__queue = [];

    /**
     * Init queue (optional).
     * @param array<T> $queueItems
     *
     * @return void
     */
    public function __construct(array $queueItems = [])
    {
        if (is_array($queueItems)) {
            $this->__queue = $queueItems;
        }
    }

    /**
     * Clears all items in queue.
     */
    public function clear(): void
    {
        $this->__queue = [];
    }

    /**
     * Check if queue contains item.
     */
    public function contains(mixed $item): bool
    {
        return in_array($item, $this->__queue, true);
    }

    /**
     * Gets front item in queue and removes from queue.
     */
    public function pop(): mixed
    {
        if ($this->isEmpty()) {
            return false;
        }

        return array_shift($this->__queue);
    }

    /**
     * Add item to back of queue, if item is not NULL.
     */
    public function push(mixed $item): void
    {
        if (null !== $item) {
            $this->__queue[] = $item;
        }
    }

    /**
     * Gets front item in queue without removing it from queue.
     */
    public function peek(): mixed
    {
        return current($this->__queue);
    }

    /**
     * Checks if queue is empty.
     */
    public function isEmpty(): bool
    {
        return 0 === count($this->__queue);
    }

    /**
     * Countable implementation - returns number of items is the queue.
     */
    public function count(): int
    {
        return count($this->__queue);
    }
}
