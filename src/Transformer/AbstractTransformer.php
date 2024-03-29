<?php declare(strict_types = 1);

/**
 * @author andis @ 23.11.2022
 */

namespace WhiteDigital\EtlBundle\Transformer;

use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Contracts\Service\Attribute\Required;
use WhiteDigital\EtlBundle\Exception\EtlException;
use WhiteDigital\EtlBundle\Exception\TransformerException;
use WhiteDigital\EtlBundle\Helper\EtlValidator;
use WhiteDigital\EtlBundle\Helper\ValidatorType;

abstract class AbstractTransformer implements TransformerInterface
{
    protected EntityManagerInterface $entityManager;

    protected OutputInterface $output;
    /** @var array<EtlValidator> */
    private array $validators = [];
    /** @var string[] */
    private array $validatorFailures = [];

    /** @var array<string, mixed> */
    private array $options;

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

    public function getOption(string $key): mixed
    {
        return $this->options[$key] ?? null;
    }

    public function displayStartupMessage(): void
    {
        $this->output->writeln(sprintf("\n<info>%s</info> uzsākts\n", static::class));
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

    protected function addValidator(EtlValidator $validator): void
    {
        $this->validators[] = $validator;
    }

    /**
     * @throws EtlException
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
                throw new TransformerException(sprintf('Validator [%s] failed', $validator->getDescription()));
            }
            $validated = $validated && $passed;
        }

        return $validated;
    }

    private function addValidatorFailure(string $validator): void
    {
        if (!array_key_exists($validator, $this->validatorFailures)) {
            $this->validatorFailures[$validator] = 1;
        } else {
            $this->validatorFailures[$validator]++;
        }
    }
}
