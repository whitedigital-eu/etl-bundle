<?php

namespace WhiteDigital\EtlBundle;

use Exception;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\TaggedLocator;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Mailer\Exception\TransportExceptionInterface;
use WhiteDigital\Audit\AuditBundle;
use WhiteDigital\Audit\Contracts\AuditServiceInterface;
use WhiteDigital\EtlBundle\Exception\EtlException;
use WhiteDigital\EtlBundle\Extractor\ExtractorInterface;
use WhiteDigital\EtlBundle\Helper\NotificationService;
use WhiteDigital\EtlBundle\Helper\Queue;
use WhiteDigital\EtlBundle\Helper\RepositoryCacheService;
use WhiteDigital\EtlBundle\Loader\LoaderInterface;
use WhiteDigital\EtlBundle\Transformer\TransformerInterface;

class EtlPipeline
{
    private string $pipelineId = '';
    private ?OutputInterface $output = null;

    private ?ExtractorInterface $extractor = null;
    private ?TransformerInterface $transformer = null;
    private ?LoaderInterface $loader = null;

    /** @noinspection PhpInapplicableAttributeTargetDeclarationInspection */
    public function __construct(
        private readonly NotificationService                                     $notificationService,
        private readonly RepositoryCacheService                                  $repositoryCache,
        private readonly AuditServiceInterface                                   $audit,
        #[TaggedLocator(tag: 'etl.extractor')] private readonly ServiceLocator   $extractors,
        #[TaggedLocator(tag: 'etl.transformer')] private readonly ServiceLocator $transformers,
        #[TaggedLocator(tag: 'etl.loader')] private readonly ServiceLocator      $loaders,
    )
    {
    }


    /**
     * @param class-string $className
     * @param array<string, mixed> $options
     * @return $this
     * @throws EtlException
     */
    public function addExtractor(string $className, array $options = []): static
    {
        try {
            $this->extractor = $this->extractors->get($className);
        } catch (NotFoundExceptionInterface|ContainerExceptionInterface $e) {
            throw new EtlException(sprintf('%s not found in available extractors: [%s]', $className, $e->getMessage()));
        }
        $this->extractor->setOutput($this->output);
        $this->extractor->setOptions($options);

        return $this;
    }


    /**
     * @param class-string $className
     * @param array<string, mixed> $options
     * @return $this
     * @throws EtlException
     */
    public function addTransformer(string $className, array $options = []): static
    {
        try {
            $this->transformer = $this->transformers->get($className);
        } catch (NotFoundExceptionInterface|ContainerExceptionInterface $e) {
            throw new EtlException(sprintf('%s not found in available transformers: [%s]', $className, $e->getMessage()));
        }
        $this->transformer->setOutput($this->output);
        $this->transformer->setOptions($options);

        return $this;
    }

    /**
     * @throws EtlException
     */
    public function addLoader(string $className, array $options = []): static
    {
        try {
            $this->loader = $this->loaders->get($className);
        } catch (NotFoundExceptionInterface|ContainerExceptionInterface $e) {
            throw new EtlException(sprintf('%s not found in available loaders: [%s]', $className, $e->getMessage()));
        }
        $this->loader->setOutput($this->output);
        $this->loader->setOptions($options);

        return $this;
    }

    /**
     * set output method - currently only ConsoleOutput used.
     *
     * @return $this
     */
    public function setOutput(OutputInterface $output): static
    {
        $this->output = $output;

        return $this;
    }

    public function setPipelineId(string $pipelineId): static
    {
        $this->pipelineId = $pipelineId;

        return $this;
    }

    public function setNotificationContext(string $context): static
    {
        $this->notificationService->addNotificationContext($context);

        return $this;
    }

    /**
     * @return $this
     * @deprecated remove this method, as it only would help in batch mode, but then it may violate unique indexes
     *
     */
    public function storeRepositoryCache(string $class, array $fetchEager, string $indexProperty): static
    {
        $this->output->writeln("Tiek iegūti esošie dati no $class");
        $this->repositoryCache->storeExistingRecords($class, $fetchEager, $indexProperty);

        return $this;
    }

    /**
     * @throws Exception|TransportExceptionInterface
     */
    public function run(bool $batchMode = false): bool
    {
        if (!$this->output) {
            throw new EtlException('ETL output not set');
        }
        if ('' === $this->pipelineId) {
            $this->output->writeln('Please set pipeline_id');

            return false;
        }
        if (!$this->extractor) {
            $this->output->writeln('Please set extractor');

            return false;
        }
        if (!$this->transformer) {
            $this->output->writeln('Please set transformer');

            return false;
        }
        if (!$this->loader) {
            $this->output->writeln('Please set loader');

            return false;
        }
        $message = sprintf('Datu ielāde [%s] uzsākta', $this->pipelineId);
        $this->output->writeln($message);
        $this->audit->audit(AuditBundle::ETL, $message);

        // EXTRACT -> TRANSFORM -> LOAD
        try {
            $this->extractor->displayStartupMessage();
            if (!$batchMode) {
                $queue = $this->extractor->run();
                if (null === $queue) {
                    throw new EtlException('Extractor must return Queue object in non-batch mode.');
                }
                $this->transformer->displayStartupMessage();
                $queue = $this->transformer->run($queue);
                $this->transformer->printValidatorFailures();
                $this->loader->displayStartupMessage();
                $this->loader->run($queue);
            } else { // Batch mode
                $this->extractor->run(function (Queue $queue) {
                    $this->transformer->displayStartupMessage();
                    $queue = $this->transformer->run($queue);
                    $this->transformer->printValidatorFailures();
                    $this->loader->displayStartupMessage();
                    $this->loader->run($queue);
                });
            }
            $this->notificationService->sendNotifications($this->output);
        } catch (Exception $exception) {
            $message = sprintf('<error>ETL [%s] neizdevās ar kļūdu: %s: %s</error>', $this->pipelineId, $exception::class, $exception->getMessage());
            $this->output->writeln("\n" . $message);
            $this->output->writeln(sprintf('<error>%s:%s</error>', $exception->getFile(), $exception->getLine()));
            $this->audit->audit(AuditBundle::ETL, $message, [
                'file' => $exception->getFile(),
                'line' => $exception->getLine(),
                'trace' => $exception->getTraceAsString(),
            ]);

            return false;
        }

        $message = sprintf('Datu ielāde [%s] pabeigta', $this->pipelineId);
        $this->output->writeln($message);
        $this->audit->audit(AuditBundle::ETL, $message);

        return true;
    }
}
