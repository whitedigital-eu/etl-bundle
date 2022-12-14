<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

use App\Entity\Classifier;
use App\Entity\Enum\ClassifierTypeEnum;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Contracts\Service\Attribute\Required;
use WhiteDigital\EntityResourceMapper\Entity\BaseEntity;
use WhiteDigital\EtlBundle\Exception\EtlException;
use WhiteDigital\EtlBundle\Exception\ExtractorException;
use WhiteDigital\EtlBundle\Exception\LoaderException;
use WhiteDigital\EtlBundle\Exception\TransformerException;
use WhiteDigital\EtlBundle\Extractor\ExtractorInterface;
use WhiteDigital\EtlBundle\Loader\LoaderInterface;
use WhiteDigital\EtlBundle\Transformer\TransformerInterface;

/**
 * @deprecated
 */
abstract class BasePipelineProcessor
{
    protected OutputInterface $output;
    /** @var string[] */
    private array $options;
    /** @var array<EtlValidator> */
    private array $validators = [];
    /** @var string[] */
    private array $validatorFailures = [];
    private EntityManagerInterface $entityManager;

    #[Required]
    public function setEntityManager(EntityManagerInterface $entityManager): void
    {
        $this->entityManager = $entityManager;
    }

    public function setOutput(OutputInterface $output): void
    {
        $this->output = $output;
    }

    /**
     * @param string[] $options
     */
    public function setOptions(array $options): void
    {
        $this->options = $options;
    }

    protected function runBase(): void
    {
        $caller = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS)[1];
        $this->output->writeln("\n");
        $this->output->writeln(sprintf('%s::%s uzsākts', $caller['class'], $caller['function']));
        $this->validatorFailures = [];
    }

    /**
     * @throws EtlException
     * @throws ExtractorException
     * @throws LoaderException
     * @throws TransformerException
     */
    protected function getOptions(string $key): mixed
    {
        if (!array_key_exists($key, $this->options)) {
            $this->throwCallerException(sprintf('Requested option key [%s] not set', $key));
        }

        return $this->options[$key];
    }

    /**
     * @param class-string $class
     */
    protected function getTableName(string $class): string
    {
        $tableName = $this->entityManager->getClassMetadata($class)->getTableName();
        if ('user' === $tableName) { // reserved keywords must be double quoted in PostgreSQL
            $tableName = sprintf('"%s"', $tableName);
        }

        return $tableName;
    }



    /**
     * @throws EtlException
     * @throws ExtractorException
     * @throws LoaderException
     * @throws TransformerException
     */
    protected function runValidators(mixed $data): bool
    {
        $validated = true;
        foreach ($this->validators as $validator) {
            $passed = $validator->run($data);
            if (!$passed) {
                $this->addValidatorFailure($validator->getDescription());
            }
            if (!$passed && ValidatorType::FAIL_VALIDATOR === $validator->getType()) {
                $this->throwCallerException(sprintf('Validator [%s] failed', $validator->getDescription()));
            }
            $validated = $validated && $passed;
        }

        return $validated;
    }



    protected function addValidator(EtlValidator $validator): void
    {
        $this->validators[] = $validator;
    }

    public function printValidatorFailures(): void
    {
        if (!empty($vf = $this->validatorFailures)) {
            $this->output->writeln("\n<comment>Validācijas paziņojumi (neimportētie dati):</comment>");
            foreach ($vf as $failure => $failureCount) {
                $this->output->writeln("- [{$failure}]: [{$failureCount}]");
            }
        }
        $this->output->writeln('');
    }

    /**
     * @param class-string $class
     * @param string[]     $lookup
     */
    protected function getEntityByLookup(string $class, array $lookup): ?BaseEntity
    {
        $repository = $this->entityManager->getRepository($class);
        $entity = $repository->findOneBy($lookup);
        $this->entityManager->clear(); // Do not leave reference in EM

        return $entity;
    }

    /**
     * @throws EtlException
     * @throws ExtractorException
     * @throws LoaderException
     * @throws TransformerException
     */
    private function throwCallerException(string $message): void
    {
        if ($this instanceof ExtractorInterface) {
            throw new ExtractorException($message);
        }
        if ($this instanceof TransformerInterface) {
            throw new TransformerException($message);
        }
        if ($this instanceof LoaderInterface) {
            throw new LoaderException();
        }

        throw new EtlException($message);
    }

    private function addValidatorFailure(string $validator): void
    {
        if (!array_key_exists($validator, $this->validatorFailures)) {
            $this->validatorFailures[$validator] = 1;
        } else {
            ++$this->validatorFailures[$validator];
        }
    }
}
