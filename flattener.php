<?php

$messagesTxt          = file_get_contents('input.json');
$messages             = json_decode($messagesTxt, true, 512, JSON_OBJECT_AS_ARRAY)['Message'];
$fullCfgTxt           = file_get_contents('flatteners.json');
$fullCfg              = json_decode($fullCfgTxt, true, 512, JSON_OBJECT_AS_ARRAY);
$inputSignature       = $fullCfg[0]['graph']['Message'];
$destinationSignature = $fullCfg[0]['destinationMessage'];

$conversionMap = [];
fillConversionMap($conversionMap, $inputSignature);
var_dump($conversionMap);die;
$mesageWithoutPartitions = $messages;
unset($mesageWithoutPartitions['partitions']);
$plainMessages = [];
foreach ($messages['partitions'] as $partition) {
    $message      = array_merge($mesageWithoutPartitions, $partition);
    $plainMessage = [];
    plainifyMessage($plainMessage, $message);
    $plainMessages[] = $plainMessage;
}

$destinationMessages = [];
foreach ($plainMessages as $plainMessage) {
    $destMessage = $destinationSignature;
    fillDestMessageWithData($destMessage, $conversionMap, $plainMessage);
    $destinationMessages[] = $destMessage;
}
print_r($destinationMessages);

function fillConversionMap(&$conversionMap, $inputSignature)
{
    foreach ($inputSignature as $fieldName => $placeholder) {
        if (is_iterable($placeholder)) {
            fillConversionMap($conversionMap, $placeholder);
        } else {
            $conversionMap[$placeholder] = $fieldName;
        }
    }
}

function plainifyMessage(&$plainMessage, $message)
{
    foreach ($message as $fieldName => $fieldValue) {
        if (is_iterable($fieldValue)) {
            plainifyMessage($plainMessage, $fieldValue);
        } else {
            $plainMessage[$fieldName] = $fieldValue;
        }
    }
}

function fillDestMessageWithData(&$destMessage, $conversionMap, $plainMessage)
{
    foreach ($destMessage as $fieldName => $placeholder) {
        if (is_iterable($placeholder)) {
            fillDestMessageWithData($destMessage[$fieldName], $conversionMap, $plainMessage);
        } else {
            if (array_key_exists($placeholder, $conversionMap)) {
                $inputMessageKey = $conversionMap[$placeholder];
                if (array_key_exists($inputMessageKey, $plainMessage)) {
                    $valueToInsert = $plainMessage[$inputMessageKey];
                    $destMessage[$fieldName] = $valueToInsert;
                    continue;
                }
            }
        }
    }
}
