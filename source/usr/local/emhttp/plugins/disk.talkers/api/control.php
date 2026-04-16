<?php
declare(strict_types=1);

header('Content-Type: application/json');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') !== 'POST') {
    http_response_code(405);
    echo json_encode([
        'ok' => false,
        'error' => 'POST required.',
    ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

$action = (string) ($_POST['action'] ?? '');
$command = '';
$message = '';

switch ($action) {
    case 'spinup_all':
        $command = "emcmd 'cmdSpinupAll=Apply'";
        $message = 'Spin up requested for all array disks.';
        break;
    case 'spindown_all':
        $command = "emcmd 'cmdSpindownAll=Apply'";
        $message = 'Spin down requested for all array disks.';
        break;
    default:
        http_response_code(400);
        echo json_encode([
            'ok' => false,
            'error' => 'Unsupported action.',
        ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
        exit;
}

exec($command, $output, $code);
if ($code !== 0) {
    http_response_code(500);
    echo json_encode([
        'ok' => false,
        'error' => 'Action failed.',
        'code' => $code,
    ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

echo json_encode([
    'ok' => true,
    'message' => $message,
], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);

