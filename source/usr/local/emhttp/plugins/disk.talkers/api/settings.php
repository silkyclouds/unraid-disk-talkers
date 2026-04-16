<?php
declare(strict_types=1);

header('Content-Type: application/json');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');

require_once __DIR__ . '/common.php';

if (($_SERVER['REQUEST_METHOD'] ?? 'GET') !== 'POST') {
    http_response_code(405);
    echo json_encode([
        'ok' => false,
        'error' => 'POST required.',
    ], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
    exit;
}

$config = read_plugin_cfg();
$defaults = disk_talkers_defaults();

$intField = static function (string $key, int $min, int $max, int $fallback): string {
    $value = isset($_POST[$key]) ? (int) $_POST[$key] : $fallback;
    return (string) max($min, min($max, $value));
};

$floatField = static function (string $key, float $min, float $max, float $fallback): string {
    $value = isset($_POST[$key]) ? (float) $_POST[$key] : $fallback;
    return rtrim(rtrim(sprintf('%.4F', max($min, min($max, $value))), '0'), '.');
};

$timeField = static function (string $key, string $fallback): string {
    $value = trim((string) ($_POST[$key] ?? $fallback));
    return preg_match('/^([01]?\d|2[0-3]):([0-5]\d)$/', $value) ? $value : $fallback;
};

$tariffMode = (string) ($_POST['electricity_tariff_mode'] ?? $defaults['ELECTRICITY_TARIFF_MODE']);
if (!in_array($tariffMode, ['single', 'dual'], true)) {
    $tariffMode = $defaults['ELECTRICITY_TARIFF_MODE'];
}

$currencySymbol = trim((string) ($_POST['currency_symbol'] ?? $defaults['CURRENCY_SYMBOL']));
if ($currencySymbol === '') {
    $currencySymbol = $defaults['CURRENCY_SYMBOL'];
}
$currencySymbol = function_exists('mb_substr') ? mb_substr($currencySymbol, 0, 4) : substr($currencySymbol, 0, 4);

$config['POLL_INTERVAL'] = $intField('poll_interval', 1, 3600, (int) ($config['POLL_INTERVAL'] ?? $defaults['POLL_INTERVAL']));
$config['RECENT_WINDOW'] = $intField('recent_window', 30, 86400, (int) ($config['RECENT_WINDOW'] ?? $defaults['RECENT_WINDOW']));
$config['MAX_TALKERS'] = $intField('max_talkers', 1, 20, (int) ($config['MAX_TALKERS'] ?? $defaults['MAX_TALKERS']));
$config['HISTORY_SAMPLE_INTERVAL'] = $intField('history_sample_interval', 60, 86400, (int) ($config['HISTORY_SAMPLE_INTERVAL'] ?? $defaults['HISTORY_SAMPLE_INTERVAL']));
$config['HDD_POWER_MIN_W'] = $floatField('hdd_power_min_w', 0.0, 40.0, (float) ($config['HDD_POWER_MIN_W'] ?? $defaults['HDD_POWER_MIN_W']));
$config['HDD_POWER_MAX_W'] = $floatField('hdd_power_max_w', 0.0, 40.0, (float) ($config['HDD_POWER_MAX_W'] ?? $defaults['HDD_POWER_MAX_W']));
$config['CURRENCY_SYMBOL'] = $currencySymbol;
$config['ELECTRICITY_TARIFF_MODE'] = $tariffMode;
$config['ELECTRICITY_SINGLE_RATE'] = $floatField('electricity_single_rate', 0.0, 10.0, (float) ($config['ELECTRICITY_SINGLE_RATE'] ?? $defaults['ELECTRICITY_SINGLE_RATE']));
$config['ELECTRICITY_PEAK_RATE'] = $floatField('electricity_peak_rate', 0.0, 10.0, (float) ($config['ELECTRICITY_PEAK_RATE'] ?? $defaults['ELECTRICITY_PEAK_RATE']));
$config['ELECTRICITY_OFFPEAK_RATE'] = $floatField('electricity_offpeak_rate', 0.0, 10.0, (float) ($config['ELECTRICITY_OFFPEAK_RATE'] ?? $defaults['ELECTRICITY_OFFPEAK_RATE']));
$config['ELECTRICITY_OFFPEAK_START'] = $timeField('electricity_offpeak_start', (string) ($config['ELECTRICITY_OFFPEAK_START'] ?? $defaults['ELECTRICITY_OFFPEAK_START']));
$config['ELECTRICITY_OFFPEAK_END'] = $timeField('electricity_offpeak_end', (string) ($config['ELECTRICITY_OFFPEAK_END'] ?? $defaults['ELECTRICITY_OFFPEAK_END']));
$config['SHOW_MAIN_SHORTCUT'] = isset($_POST['show_main_shortcut']) && $_POST['show_main_shortcut'] === '1' ? '1' : '0';

if ((float) $config['HDD_POWER_MAX_W'] < (float) $config['HDD_POWER_MIN_W']) {
    $config['HDD_POWER_MAX_W'] = $config['HDD_POWER_MIN_W'];
}

write_plugin_cfg($config);

$restart = '/usr/local/emhttp/plugins/disk.talkers/scripts/rc.disk.talkers restart >/dev/null 2>&1';
exec($restart);

echo json_encode([
    'ok' => true,
    'settings' => disk_talkers_settings_payload($config),
], JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);
