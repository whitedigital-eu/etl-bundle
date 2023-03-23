<?php

declare(strict_types = 1);

namespace WhiteDigital\EtlBundle\Helper;

use RuntimeException;
use Symfony\Bridge\Twig\Mime\TemplatedEmail;
use Symfony\Bundle\SecurityBundle\Security;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Mailer\Exception\TransportExceptionInterface;
use Symfony\Component\Mailer\MailerInterface;
use Symfony\Component\PropertyAccess\PropertyAccess;
use Throwable;
use WhiteDigital\Audit\Contracts\AuditServiceInterface;
use WhiteDigital\Audit\Contracts\AuditType;

final class NotificationService
{
    private array $notifications;
    private string $context;

    public function __construct(
        private readonly MailerInterface $mailer,
        private readonly AuditServiceInterface $audit,
        private readonly Security $security,
    ) {
    }

    /**
     * @throws TransportExceptionInterface
     */
    public function sendNotifications(OutputInterface $output): void
    {
        if (empty($this->notifications)) {
            return; // nothing to send
        }

        $output->writeln(sprintf('Uzsākta e-pasta paziņojuma gatavošana par %s ierakstiem', count($this->notifications)));
        $notificationData = $this->mergeIdentical($this->notifications);

        $currentEmail = $this->security->getUser()?->getUserIdentifier() ?? 'info@whitedigital.eu';
        $email = (new TemplatedEmail())
            ->to($currentEmail)
            ->subject('Valtek CRM: Datu importa paziņojums')
            ->htmlTemplate('etl_notification_service/email.html.twig')
            ->context([
                'context' => $this->context,
                'notificationData' => $notificationData,
            ]);

        $this->mailer->send($email);
        $output->writeln("E-pasta ziņojums nosūtīts adresātam {$currentEmail}");
        $this->audit->audit(AuditType::ETL, "Notifications sent to $currentEmail", ['notification' => $notificationData]);
    }

    public function addNotificationContext(string $context): void
    {
        $this->context = $context;
    }

    public function addNotification(string $primaryKeyName, string $primaryKey, array $afterChangesFields, object $beforeChanges): void
    {
        if (empty($afterChangesFields)) {
            return;
        }
        // TODO Move entity property accessor to DBalHelper class and use also in returnUpdatedFields and maybe other methods.
        $propertyAccessor = PropertyAccess::createPropertyAccessor();
        $beforeChangesFields = [];
        foreach ($afterChangesFields as $key => $value) {
            if (str_ends_with($key, 'Id')) { // It is ORM relation. For example customerId, we should get customer->getId()
                $relationName = substr($key, 0, -2);
                $getterMethod = 'get' . ucfirst($relationName);
                if (!method_exists($beforeChanges, $getterMethod)) {
                    throw new RuntimeException("{$getterMethod} does not exist in beforeChanges object.");
                }
                try {
                    $beforeChangesFields[$key] = $beforeChanges->{$getterMethod}()?->getId();
                } catch (Throwable $error) {
                    $beforeChangesFields[$key] = null;
                }
            } else {
                $beforeChangesFields[$key] = $propertyAccessor->getValue($beforeChanges, $key);
            }
        }
        $this->notifications["{$primaryKeyName}: {$primaryKey}"][] = [
            'before' => $this->expandArray($beforeChangesFields),
            'after' => $this->expandArray($afterChangesFields),
        ];
    }

    /**
     * Convert array key value pairs to continuous string.
     */
    private function expandArray(array $data): string
    {
        $output = '';
        if (empty($data)) {
            return '';
        }
        foreach ($data as $key => $value) {
            $output .= "<u>$key</u>: $value; ";
        }

        return $output;
    }

    private function mergeIdentical(array $data): array
    {
        $output = [];
        foreach ($data as $index => $items) {
            $element = [
                'key' => $index,
                'before' => '',
                'after' => '',
            ];
            foreach ($items as $item) {
                $element['before'] .= $item['before'];
                $element['after'] .= $item['after'];
            }
            $output[] = $element;
        }

        return $output;
    }
}
