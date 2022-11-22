<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Command\Output;

use Symfony\Component\Console\Output\Output;

class EventSourceOutput extends Output
{
    protected function doWrite(string $message, bool $newline): void
    {
        $message = strip_tags($message); // Remove tags used for CLI output coloring
        $message = str_replace("\n", '<br />', $message);
        $event = [];
        $event[] = sprintf('id: %s', str_replace('.', '', uniqid('', true)));
        $event[] = sprintf('retry: %s', 0);
        $event[] = sprintf('event: %s', 'console');
        $event[] = sprintf('data: %s', $message.($newline ? '<br />' : ''));
        echo implode("\n", $event)."\n\n";
        if (ob_get_level() > 0) {
            ob_flush();
        }
        flush();
    }
}
