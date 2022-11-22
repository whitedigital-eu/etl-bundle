<?php

namespace WhiteDigital\EtlBundle;

use App\Service\AuditService;
use Exception;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\DependencyInjection\Attribute\TaggedLocator;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Mailer\Exception\TransportExceptionInterface;
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

    private ServiceLocator $extractors;
    private ServiceLocator $transformers;
    private ServiceLocator $loaders;

    /**
     * @param ServiceLocator $extractors
     * @param ServiceLocator $transformers
     * @param ServiceLocator $loaders
     */
    public function __construct(
        private readonly NotificationService                        $notificationService,
        private readonly RepositoryCacheService                     $repositoryCache,
        private readonly AuditService                               $audit,
        #[TaggedLocator(tag: 'etl.etl_extractor')] ServiceLocator   $extractors,
        #[TaggedLocator(tag: 'etl.etl_transformer')] ServiceLocator $transformers,
        #[TaggedLocator(tag: 'etl.etl_loader')] ServiceLocator      $loaders,
    ) {
        $this->extractors = $extractors;
        $this->transformers = $transformers;
        $this->loaders = $loaders;
    }

    /**
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
     * @deprecated remove this method, as it only would help in batch mode, but then it may violate unique indexes
     *
     * @return $this
     */
    public function storeRepositoryCache(string $class, array $fetchEager, string $indexProperty): static
    {
        $this->output->writeln("Tiek iegūti esošie dati no {$class}");
        $this->repositoryCache->storeExistingRecords($class, $fetchEager, $indexProperty);

        return $this;
    }

    /**
     * @throws Exception|TransportExceptionInterface
     */
    public function run(bool $batch_mode = false): bool
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
        $this->audit->audit(AuditService::CATEGORY_ETL_PIPELINE, $message);

        // EXTRACT -> TRANSFORM -> LOAD
        try {
            if (!$batch_mode) {
                $queue = $this->extractor->run();
                if (null === $queue) {
                    throw new EtlException('Extractor must return Queue object in non-batch mode.');
                }
                $queue = $this->transformer->run($queue);
                $this->loader->run($queue);
            } else {
                $this->extractor->run(function (Queue $queue) {
                    $queue = $this->transformer->run($queue);
                    $this->loader->run($queue);
                });
            }
            $this->notificationService->sendNotifications($this->output);
        } catch (Exception $exception) {
            $message = sprintf('<error>ETL [%s] neizdevās ar kļūdu: %s: %s</error>', $this->pipelineId, $exception::class, $exception->getMessage());
            $this->output->writeln("\n".$message);
            $this->output->writeln(sprintf('<error>%s:%s</error>', $exception->getFile(), $exception->getLine()));
            $this->audit->audit(AuditService::CATEGORY_ETL_PIPELINE, $message, [
                'file' => $exception->getFile(),
                'line' => $exception->getLine(),
                'trace' => $exception->getTraceAsString(),
            ]);

            return false;
        }

        $message = sprintf('Datu ielāde [%s] pabeigta', $this->pipelineId);
        $this->output->writeln($message);
        $this->audit->audit(AuditService::CATEGORY_ETL_PIPELINE, $message);

        return true;
    }
}
