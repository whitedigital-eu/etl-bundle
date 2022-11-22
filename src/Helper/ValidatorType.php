<?php

declare(strict_types=1);

namespace WhiteDigital\EtlBundle\Helper;

enum ValidatorType
{
    case SKIP_VALIDATOR;
    case FAIL_VALIDATOR;
}
