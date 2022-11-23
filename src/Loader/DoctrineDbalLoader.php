<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Loader;

use App\Enum\AuditCategoryEnum;
use App\Service\AuditService;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Query\QueryBuilder;
use Doctrine\Persistence\ManagerRegistry;
use Exception;
use WhiteDigital\EtlBundle\Command\Output\WebProgressBar;
use WhiteDigital\EtlBundle\Exception\LoaderException;
use WhiteDigital\EtlBundle\Helper\CallbackQuery;
use WhiteDigital\EtlBundle\Helper\Queue;

/**
 * Receives Queue object and processes each element.
 * Element can be either instance of QueryBuilder (single DBAL query) or CallbackQuery (callback function to execute)
 * Loop is wrapped inside transaction.
 */
class DoctrineDbalLoader extends AbstractLoader
{
    public function __construct(
        private readonly AuditService    $audit,
        private readonly ManagerRegistry $doctrine,
    )
    {
    }

    /**
     * @param Queue<object> $data
     *
     * @throws LoaderException
     * @throws \Doctrine\DBAL\Exception
     */
    public function run(Queue $data): void
    {
        if (0 === count($data)) {
            $this->output->writeln('Nav izpildāmu datu bāzes vaicājumu.');

            return;
        }
        $queryLog = [];
        $numberUpdates = 0;
        $numberInserts = 0;
        /** @var Connection $connection */
        $connection = $this->doctrine->getConnection();
        $connection->beginTransaction();
        try {
            $progressBar = new WebProgressBar($this->output, count($data));
            $this->output->writeln('Izpilda datu bāzes vaicājumus');
            $progressBar->start();
            while ($record = $data->pop()) {
                if ($record instanceof QueryBuilder) { // Insert or Update Query
                    $record->executeQuery();

                    if (QueryBuilder::INSERT === $record->getType()) {
                        ++$numberInserts;
                    } elseif (QueryBuilder::UPDATE === $record->getType()) {
                        ++$numberUpdates;
                    } else {
                        throw new LoaderException(sprintf('Unsupported query type (%s) received.', $record->getType()));
                    }
                    $queryLog[] = ['q' => match ($record->getType()) {
                        QueryBuilder::INSERT => 'Insert',
                        QueryBuilder::UPDATE => 'Update'
                    }, 'p' => $record->getParameters()];
                } elseif ($record instanceof CallbackQuery) { // Callback
                    $statistics = $record->execute();
                    $numberInserts += $statistics['insert'];
                    $numberUpdates += $statistics['update'];
                    if (!empty($log = $statistics['log'])) {
                        $queryLog[] = $log;
                    }
                } else {
                    throw new LoaderException(sprintf('Unknown type (%s) received in Loader, expecting DBAL\QueryBuilder', gettype($record)));
                }
                $progressBar->advance();
            }
            $progressBar->finish();
            $this->output->writeln(sprintf("\nDatu bāzes vaicājumi pabeigti ar %s INSERT and %s UPDATE operācijām.\n", $numberInserts, $numberUpdates));
            $connection->commit();
            if ($numberInserts > 0 || $numberUpdates > 0) {
                $this->audit->audit(AuditCategoryEnum::ETLPipeline, sprintf('Loader query log with %s INSERTs and %s UPDATEs', $numberInserts, $numberUpdates), $queryLog);
            }
        } catch (Exception $exception) {
            $connection->rollBack();
            throw $exception;
        }
    }
}
