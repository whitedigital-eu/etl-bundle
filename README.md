# Extract Transform Load process Symfony bundle

Package for running ETL tasks.  
1. Define custom Extractors, Transformers and Loaders
2. Create pipeline to run specific data import/export process.

Tasks can be run:  
1. from CLI (bin/console etl:run <task_name>)
2. from services or controllers
3. from frontend using Server Sent Events (SSE) API.

## Requirements

1. PHP 8.1+  
2. Symfony 6.1+  

## Install bundle
```bash 
composer req "whitedigital-eu/etl-bundle"
```

## Setup task
Example task (HorizonDataExtractor, HorizonCustomerTransformer - should be created separately) 
```php 
<?php

declare(strict_types=1);

namespace App\ETL\Task;

use App\ETL\Extractor\HorizonDataExtractor;
use App\ETL\Transformer\HorizonCustomerTransformer;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Mailer\Exception\TransportExceptionInterface;
use WhiteDigital\EtlBundle\Attribute\AsTask;
use WhiteDigital\EtlBundle\Exception\EtlException;
use WhiteDigital\EtlBundle\Loader\DoctrineDbalLoader;
use WhiteDigital\EtlBundle\Task\AbstractTask;

#[AsTask(name: 'horizon_customer_import')]
class HorizonCustomerImportTask extends AbstractTask
{
    /**
     * @throws EtlException
     * @throws TransportExceptionInterface
     */
    public function runTask(OutputInterface $output, array $extractorArgs = null): void
    {
        $this->etlPipeline
            ->setOutput($output)
            ->addExtractor(HorizonDataExtractor::class, $extractorArgs ?? ['path' => '/rest/TDdmNorSar/query?columns=K.KODS,K.NOSAUK&orderby=K.NOSAUK asc'])
            ->addTransformer(HorizonCustomerTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

}
```

Example Extractor:
```php 
<?php

declare(strict_types=1);

namespace App\ETL\Extractor;

use App\Service\HorizonRestApiService;
use Psr\Cache\InvalidArgumentException;
use Symfony\Contracts\HttpClient\Exception\ClientExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\RedirectionExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\ServerExceptionInterface;
use Symfony\Contracts\HttpClient\Exception\TransportExceptionInterface;
use WhiteDigital\EtlBundle\Exception\ExtractorException;
use WhiteDigital\EtlBundle\Extractor\AbstractExtractor;
use WhiteDigital\EtlBundle\Helper\Queue;

final class HorizonDataExtractor extends AbstractExtractor
{
    public function __construct(
        private readonly HorizonRestApiService $horizonRestApiService,
    )
    {
    }

    /**
     * @param \Closure|null $batchProcessor
     * @return Queue<\stdClass>
     * @throws ClientExceptionInterface
     * @throws ExtractorException
     * @throws InvalidArgumentException
     * @throws RedirectionExceptionInterface
     * @throws ServerExceptionInterface
     * @throws TransportExceptionInterface
     * @throws \JsonException
     */
    public function run(\Closure $batchProcessor = null): Queue
    {
        if (null !== $batchProcessor) {
            throw new ExtractorException(sprintf('Batch mode not supported by %s', __CLASS__));
        }
        $data = new Queue();

        $this->output->writeln(sprintf('Datu iegūšana uzsākta no avota: [%s]', $path = $this->getOption('path')));
        $rawJsonData = $this->horizonRestApiService->makeGetRequest($path);
        foreach ($rawJsonData?->collection->row as $row) {
            $data->push($row);
        }
        $this->output->writeln(sprintf('Iegūti %s ieraksti.', count($data)));

        return $data;
    }
}
```
Example Transformer:
```php 
```

Example Loader:
```php  

```

## Console commands
1. Run task by its name:
```bash
bin/console etl:run horizon_customer_import
```
or pass extra custom argument:
```bash
bin/console etl:run horizon_customer_import random_path.txt
```

2. List available tasks:
```bash
bin/console etl:list
```
