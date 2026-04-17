<?php
declare(strict_types=1);

header('Content-Type: application/json');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

require_once __DIR__ . '/common.php';

$plugin = DISK_TALKERS_PLUGIN;
$configPath = DISK_TALKERS_CONFIG_PATH;
$collector = "/usr/local/emhttp/plugins/{$plugin}/scripts/collector.py";
$rcScript = "/usr/local/emhttp/plugins/{$plugin}/scripts/rc.{$plugin}";
$config = read_plugin_cfg($configPath);
$stateFile = $config['STATE_FILE'] ?? '/tmp/disk.talkers/state.json';
$interval = max(5, (int) ($config['POLL_INTERVAL'] ?? 5));
$staleThreshold = max(15, $interval * 3);
$refreshRequested = isset($_GET['refresh']);
$pythonPath = disk_talkers_find_python3();

if ($pythonPath === '') {
    echo json_encode(disk_talkers_dependency_blocked_response($config), JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

$readStatePayload = static function (string $path): array|null {
    if (!is_file($path)) {
        return null;
    }

    $raw = file_get_contents($path);
    if ($raw === false) {
        return null;
    }

    $payload = json_decode($raw, true);
    return is_array($payload) ? $payload : null;
};

clearstatcache(true, $stateFile);
$stateExists = is_file($stateFile);
$statePayload = $readStatePayload($stateFile);
$stateIsBlocked = is_array($statePayload) && disk_talkers_payload_is_dependency_blocked($statePayload);
$stateWasBlocked = $stateIsBlocked;
$stateIsStale = !$stateExists || (time() - filemtime($stateFile)) > $staleThreshold;
$needsRefresh = $refreshRequested || $stateIsBlocked || $stateIsStale;

if ($stateWasBlocked && is_file($rcScript)) {
    $cmd = sprintf('/bin/bash %s once >/dev/null 2>&1', escapeshellarg($rcScript));
    exec($cmd);
    clearstatcache(true, $stateFile);

    $statePayload = $readStatePayload($stateFile);
    $stateIsBlocked = is_array($statePayload) && disk_talkers_payload_is_dependency_blocked($statePayload);
    if (!$stateIsBlocked) {
        $cmd = sprintf('/bin/bash %s start >/dev/null 2>&1', escapeshellarg($rcScript));
        exec($cmd);
        clearstatcache(true, $stateFile);
    }
} else {
    if ($needsRefresh && is_file($rcScript)) {
        $cmd = sprintf('/bin/bash %s start >/dev/null 2>&1', escapeshellarg($rcScript));
        exec($cmd);
        clearstatcache(true, $stateFile);
    }

    $stateExists = is_file($stateFile);
    $stateIsStale = !$stateExists || (time() - filemtime($stateFile)) > $staleThreshold;
    if (($refreshRequested || $stateIsStale) && is_file($rcScript)) {
        $cmd = sprintf('/bin/bash %s once >/dev/null 2>&1', escapeshellarg($rcScript));
        exec($cmd);
        clearstatcache(true, $stateFile);
    } elseif (($refreshRequested || $stateIsStale) && is_file($collector)) {
        $cmd = sprintf(
            'python3 %s --once --config %s --state-file %s >/dev/null 2>&1',
            escapeshellarg($collector),
            escapeshellarg($configPath),
            escapeshellarg($stateFile)
        );
        exec($cmd);
        clearstatcache(true, $stateFile);
    }
}

if ($stateWasBlocked && !is_file($rcScript) && is_file($collector)) {
    $cmd = sprintf(
        'python3 %s --once --config %s --state-file %s >/dev/null 2>&1',
        escapeshellarg($collector),
        escapeshellarg($configPath),
        escapeshellarg($stateFile)
    );
    exec($cmd);
    clearstatcache(true, $stateFile);
}

if (!is_file($stateFile)) {
    echo json_encode([
        'ok' => true,
        'initializing' => true,
        'generated_at' => gmdate(DATE_ATOM),
        'collector_mode' => 'starting',
        'warnings' => [
            'Collector is starting. Data should appear within a few seconds.',
        ],
        'mount_audit' => [],
        'array_talkers' => [],
        'disks' => [],
        'history' => [
            'default_period' => 'daily',
            'periods' => new stdClass(),
        ],
        'settings' => disk_talkers_settings_payload($config),
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
