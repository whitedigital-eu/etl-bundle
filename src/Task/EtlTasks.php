<?php

namespace WhiteDigital\EtlBundle\Task;

use Exception;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Mailer\Exception\TransportExceptionInterface;
use WhiteDigital\EtlBundle\EtlPipeline;
use WhiteDigital\EtlBundle\Exception\EtlException;

class EtlTasks
{
    public const HORIZON_CUSTOMER = 'horizon_customer_importer';
    public const HORIZON_CONTACT_PERSON = 'horizon_contact_person_importer';
    public const HORIZON_ADDRESS = 'horizon_address_importer';
    public const HORIZON_CONTRACT_EQUIPMENT = 'horizon_contract_equipment_importer';
    public const VTUA_EXCEL_CUSTOMER_EQUIPMENT = 'vtua_excel_customer_equipment_importer';
    public const HORIZON_USERS = 'horizon_users_importer';
    public const LURSOFT_ANIMALS = 'lursoft_animal_registry';
    public const LAD_LAND_AREA = 'lad_land_area';

    public function __construct(
        private readonly EtlPipeline $etlPipeline,
    ) {
    }

    /**
     * @throws Exception
     * @throws TransportExceptionInterface
     */
    public function runTaskById(string $task_id, OutputInterface $output, $filePath = null): bool
    {
        return match ($task_id) {
            self::HORIZON_CUSTOMER => $this->taskHorizonCustomerImport($output),
            self::HORIZON_CONTACT_PERSON => $this->taskHorizonContactPersonImport($output),
            self::HORIZON_ADDRESS => $this->taskHorizonAddressImport($output),
            self::HORIZON_CONTRACT_EQUIPMENT => $this->taskHorizonContractEquipmentImport($output),
            self::VTUA_EXCEL_CUSTOMER_EQUIPMENT => $this->taskVtuaExcelCustomerEquipmentImport($output, $filePath),
            self::HORIZON_USERS => $this->taskHorizonUsersImport($output),
            self::LURSOFT_ANIMALS => $this->taskLursoftAnimalsImport($output, $filePath),
            self::LAD_LAND_AREA => $this->taskLadAreaImport($output, $filePath),
            default => throw new \WhiteDigital\EtlBundle\Exception\EtlException("Task {$task_id} not found."),
        };
    }

    /**
     * @throws EtlException
     * @throws Exception
     * @throws TransportExceptionInterface
     */
    private function taskHorizonAddressImport(OutputInterface $output): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::HORIZON_ADDRESS)
            ->setOutput($output)
            ->addExtractor(HorizonDataExtractor::class, ['path' => '/rest/TdmNAdresesSar/query?columns=A_PK_ADRESE,A_PK_KLIENTS,A_LEG_ADR,A_POST_ADR,A_ADRESE1,A_ADRESE2,A_ADRESE3,A_PASTA_IND,A_PK_VALSTS,A_ADRESE'])
            ->addTransformer(HorizonAddressTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

    /**
     * @throws Exception
     * @throws TransportExceptionInterface
     */
    private function taskHorizonCustomerImport(OutputInterface $output): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::HORIZON_CUSTOMER)
            ->setOutput($output)
            ->addExtractor(HorizonDataExtractor::class, ['path' => '/rest/TDdmKlSar/query?filter=K_STATUSS eq 0 and TIP_NOSAUK eq Pircējs&columns=K_PK_KLIENTS,K_KODS,K_NOSAUK,K_EPASTS,K_TELEFONS,K_MOBTEL,K_REG_NR,K_PVN_REGNR,K_ADRESE,K_DBKRPAZ,K_STATUSS,K_PK_VALSTS'])
            ->addTransformer(HorizonCustomerTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

    /**
     * @throws EtlException
     * @throws Exception
     * @throws TransportExceptionInterface
     */
    private function taskHorizonContactPersonImport(OutputInterface $output): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::HORIZON_CONTACT_PERSON)
            ->setOutput($output)
            ->addExtractor(HorizonDataExtractor::class, ['path' => '/rest/TsarDKPersBaseAdv/query?columns=KP_VARDS,KP_KONTAKTP,K_KODS,K_NOSAUK,DPERSR_PAMATP,KP_TELEFONS,KP_MOBTEL,KP_EPASTS,KP_PERSKODS,K_PK_KLIENTS'])
            ->addTransformer(HorizonPersonTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

    /**
     * @throws EtlException
     * @throws Exception
     * @throws TransportExceptionInterface
     */
    private function taskHorizonContractEquipmentImport(OutputInterface $output): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::HORIZON_CONTRACT_EQUIPMENT)
            ->setOutput($output)
            ->addExtractor(HorizonDataExtractor::class, ['path' => '/rest/TdmKllig/query?filter=KLV_KODS eq SAS&columns=KLIG_STATUSS,KLIG_NUMURS,KLIG_PK_KLIENTS,KLIG_DATUMS,KLIG_DAT_BEIG,LIGIPA33_VALUE,LIGIPA34_VERTTEXT,KLV_KODS,KLV_NOSAUK'])
            ->addTransformer(HorizonContractEquipmentTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

    /**
     * @throws EtlException
     * @throws Exception
     * @throws TransportExceptionInterface
     */
    private function taskVtuaExcelCustomerEquipmentImport(OutputInterface $output, string $filePath = null): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::VTUA_EXCEL_CUSTOMER_EQUIPMENT)
            ->setOutput($output)
            ->addExtractor(ExcelDataExtractor::class, ['path' => $filePath ?? 'VTUA.xlsx'])
            ->addTransformer(VtuaCustomerEquipmentTransformer::class)
            ->setNotificationContext('VTUA faila izmaiņas tehnikas tabulā un sasaistē ar klientiem')
            ->addLoader(DoctrineDbalLoader::class)
            ->run(true);
    }

    /**
     * @throws EtlException
     * @throws Exception|TransportExceptionInterface
     */
    private function taskHorizonUsersImport(OutputInterface $output): bool
    {
        $today = (new \DateTimeImmutable())->format('Y-m-d');

        return $this->etlPipeline
            ->setPipelineId(self::HORIZON_USERS)
            ->setOutput($output)
            ->addExtractor(HorizonDataExtractor::class, ['path' => "/rest/TdmDArbSar/query?filter=LAT_SAK_DAT le '{$today}' or LAT_BEIG_DAT le '{$today}'&columns=ERS_FNAME,ERS_LNAME,ERS_EPASTS,ERS_TELEFONS,ERS_MOBTEL,PSAMA_NOSAUK"])
            ->addTransformer(HorizonUserTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

    /**
     * @throws EtlException
     * @throws TransportExceptionInterface
     */
    private function taskLursoftAnimalsImport(OutputInterface $output, string $filePath = null): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::LURSOFT_ANIMALS)
            ->setOutput($output)
            ->addExtractor(ExcelDataExtractor::class, ['path' => $filePath ?? 'Lursoft.xlsx'])
            ->addTransformer(LursoftAnimalRegistryTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }

    /**
     * @throws EtlException
     * @throws TransportExceptionInterface
     */
    private function taskLadAreaImport(OutputInterface $output, string $filePath = null): bool
    {
        return $this->etlPipeline
            ->setPipelineId(self::LAD_LAND_AREA)
            ->setOutput($output)
            ->addExtractor(ExcelDataExtractor::class, ['path' => $filePath ?? 'Lad.xlsx'])
            ->addTransformer(LadLandRegistryTransformer::class)
            ->addLoader(DoctrineDbalLoader::class)
            ->run();
    }
}
