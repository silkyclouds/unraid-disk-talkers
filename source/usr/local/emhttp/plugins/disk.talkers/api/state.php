<?php
declare(strict_types=1);

header('Content-Type: application/json');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

require_once __DIR__ . '/common.php';

$plugin = DISK_TALKERS_PLUGIN;
$configPath = DISK_TALKERS_CONFIG_PATH;
$collector = "/usr/local/emhttp/plugins/{$plugin}/scripts/collector.py";
$config = read_plugin_cfg($configPath);
$stateFile = $config['STATE_FILE'] ?? '/tmp/disk.talkers/state.json';
$interval = max(5, (int) ($config['POLL_INTERVAL'] ?? 5));
$staleThreshold = max(15, $interval * 3);
$needsRefresh = !is_file($stateFile) || (time() - filemtime($stateFile)) > $staleThreshold || isset($_GET['refresh']);

if ($needsRefresh && is_executable($collector)) {
    $cmd = sprintf(
        'python3 %s --once --config %s --state-file %s >/dev/null 2>&1',
        escapeshellarg($collector),
        escapeshellarg($configPath),
        escapeshellarg($stateFile)
    );
    exec($cmd);
}

if (!is_file($stateFile)) {
    http_response_code(503);
    echo json_encode([
        'ok' => false,
        'error' => 'State file is not available yet.',
        'generated_at' => gmdate(DATE_ATOM),
    ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

$raw = file_get_contents($stateFile);
if ($raw === false) {
    http_response_code(500);
    echo json_encode([
        'ok' => false,
        'error' => 'Failed to read state file.',
        'generated_at' => gmdate(DATE_ATOM),
    ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

$payload = json_decode($raw, true);
if (!is_array($payload)) {
    http_response_code(500);
    echo json_encode([
        'ok' => false,
        'error' => 'State payload is invalid JSON.',
        'generated_at' => gmdate(DATE_ATOM),
    ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

$payload['settings'] = disk_talkers_settings_payload($config);
echo json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
