<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Command\Output;

use Symfony\Component\Console\Output\OutputInterface;

/**
 * Inspired by Symfony\Component\Console\Helper\ProgressBar
 * More primitive progress bar with forward write only,
 * without ANSI terminal option to overwrite existing characters.
 * Will display 20 dots, 1 per each 5%:
 * [....................]:max
 * [....................]:100%.
 */
final class WebProgressBar
{
    private OutputInterface $output;
    private int $max;
    private int $position = 0;
    private int $printedPositions = 0;
    private int $maxPrintedPositions = 20;

    public function __construct(OutputInterface $output, int $max = 0)
    {
        $this->output = $output;
        $this->max = $max;
    }

    public function start(): void
    {
        $this->output->writeln('['.str_repeat('.', $this->maxPrintedPositions).']:'.$this->max);
        $this->output->write('[');
    }

    public function advance(): void
    {
        ++$this->position;
        $percentage = round($this->position / $this->max * 100, 0);
        $percentsPerPosition = 100 / $this->maxPrintedPositions;
        $pointsPerAdvance = $this->max > 100 ? 1 : (int)floor($this->maxPrintedPositions / $this->max);
        if ($percentage > ($this->printedPositions * $percentsPerPosition)) {
            $this->output->write(str_repeat('.', $pointsPerAdvance));
            $this->printedPositions += $pointsPerAdvance;
        }
    }

    public function finish(): void
    {
        if ($this->printedPositions < $this->maxPrintedPositions) {
            $toPrint = $this->maxPrintedPositions - $this->printedPositions;
            $this->output->write(str_repeat('.', $toPrint));
        }
        $this->output->writeln(']:100%');
    }
}
