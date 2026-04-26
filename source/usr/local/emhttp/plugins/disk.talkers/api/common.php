<?php
declare(strict_types=1);

const DISK_TALKERS_PLUGIN = 'disk.talkers';
const DISK_TALKERS_CONFIG_PATH = '/boot/config/plugins/disk.talkers/disk.talkers.cfg';
const DISK_TALKERS_PYTHON_PLUGIN_NAME = 'Python 3 for UNRAID (6.11+)';
const DISK_TALKERS_PYTHON_PLUGIN_SUPPORT_URL = 'https://forums.unraid.net/topic/175402-plugin-python-3-for-unraid-611/';
const DISK_TALKERS_UNRAID_PATH = '/usr/local/sbin:/usr/sbin:/sbin:/usr/local/bin:/usr/bin:/bin';

function disk_talkers_defaults(): array {
    return [
        'SERVICE' => 'enable',
        'POLL_INTERVAL' => '5',
        'RECENT_WINDOW' => '300',
        'MAX_TALKERS' => '5',
        'STATE_FILE' => '/tmp/disk.talkers/state.json',
        'LOG_FILE' => '/var/log/disk.talkers.log',
        'HISTORY_FILE' => '/boot/config/plugins/disk.talkers/history.sqlite3',
        'HISTORY_SAMPLE_INTERVAL' => '300',
        'HDD_POWER_MIN_W' => '6',
        'HDD_POWER_MAX_W' => '9',
        'CURRENCY_SYMBOL' => '€',
        'ELECTRICITY_TARIFF_MODE' => 'single',
        'ELECTRICITY_SINGLE_RATE' => '0.0',
        'ELECTRICITY_PEAK_RATE' => '0.0',
        'ELECTRICITY_OFFPEAK_RATE' => '0.0',
        'ELECTRICITY_OFFPEAK_START' => '22:00',
        'ELECTRICITY_OFFPEAK_END' => '07:00',
        'SHOW_MAIN_SHORTCUT' => '1',
    ];
}

function read_plugin_cfg(string $path = DISK_TALKERS_CONFIG_PATH): array {
    $config = [];
    if (!is_file($path)) {
        return $config;
    }

    foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) as $line) {
        $line = trim($line);
        if ($line === '' || $line[0] === '#') {
            continue;
        }

        $parts = explode('=', $line, 2);
        if (count($parts) !== 2) {
            continue;
        }

        $key = trim($parts[0]);
        $value = trim($parts[1]);
        $value = trim($value, "\"'");
        $config[$key] = $value;
    }

    return $config;
}

function disk_talkers_settings_payload(array $config): array {
    $defaults = disk_talkers_defaults();
    $merged = array_merge($defaults, $config);

    return [
        'poll_interval' => max(1, (int) $merged['POLL_INTERVAL']),
        'recent_window' => max(30, (int) $merged['RECENT_WINDOW']),
        'max_talkers' => max(1, (int) $merged['MAX_TALKERS']),
        'history_sample_interval' => max(60, (int) $merged['HISTORY_SAMPLE_INTERVAL']),
        'hdd_power_min_w' => max(0.0, (float) $merged['HDD_POWER_MIN_W']),
        'hdd_power_max_w' => max(0.0, (float) $merged['HDD_POWER_MAX_W']),
        'currency_symbol' => (string) $merged['CURRENCY_SYMBOL'],
        'electricity_tariff_mode' => in_array($merged['ELECTRICITY_TARIFF_MODE'], ['single', 'dual'], true) ? $merged['ELECTRICITY_TARIFF_MODE'] : 'single',
        'electricity_single_rate' => max(0.0, (float) $merged['ELECTRICITY_SINGLE_RATE']),
        'electricity_peak_rate' => max(0.0, (float) $merged['ELECTRICITY_PEAK_RATE']),
        'electricity_offpeak_rate' => max(0.0, (float) $merged['ELECTRICITY_OFFPEAK_RATE']),
        'electricity_offpeak_start' => (string) $merged['ELECTRICITY_OFFPEAK_START'],
        'electricity_offpeak_end' => (string) $merged['ELECTRICITY_OFFPEAK_END'],
        'show_main_shortcut' => ($merged['SHOW_MAIN_SHORTCUT'] ?? '1') === '1',
    ];
}

function write_plugin_cfg(array $config, string $path = DISK_TALKERS_CONFIG_PATH): void {
    $defaults = disk_talkers_defaults();
    $merged = array_merge($defaults, $config);
    $order = array_keys($defaults);
    $lines = [];

    foreach ($order as $key) {
        $value = (string) ($merged[$key] ?? $defaults[$key]);
        $lines[] = sprintf('%s="%s"', $key, str_replace('"', '\"', $value));
    }

    foreach ($merged as $key => $value) {
        if (in_array($key, $order, true)) {
            continue;
        }
        $lines[] = sprintf('%s="%s"', $key, str_replace('"', '\"', (string) $value));
    }

    file_put_contents($path, implode(PHP_EOL, $lines) . PHP_EOL, LOCK_EX);
}

function disk_talkers_find_python3(): string {
    $path = trim((string) shell_exec('PATH=' . escapeshellarg(DISK_TALKERS_UNRAID_PATH) . ' command -v python3 2>/dev/null'));
    return $path;
}

function disk_talkers_dependency_payload(): array {
    return [
        'name' => DISK_TALKERS_PYTHON_PLUGIN_NAME,
        'reason' => 'python3 binary not found on the Unraid host',
        'support_url' => DISK_TALKERS_PYTHON_PLUGIN_SUPPORT_URL,
        'install_hint' => 'Install `Python 3 for UNRAID (6.11+)` from Community Applications, then refresh this page.',
    ];
}

function disk_talkers_dependency_blocked_response(array $config): array {
    return [
        'ok' => true,
        'dependency_blocked' => true,
        'generated_at' => gmdate(DATE_ATOM),
        'collector_mode' => 'dependency-blocked',
        'dependency' => disk_talkers_dependency_payload(),
        'warnings' => [],
        'mount_audit' => [],
        'array_talkers' => [],
        'disks' => [],
        'history' => [
            'default_period' => 'daily',
            'periods' => new stdClass(),
        ],
        'settings' => disk_talkers_settings_payload($config),
    ];
}

function disk_talkers_payload_is_dependency_blocked(array $payload): bool {
    return !empty($payload['dependency_blocked']) || (!empty($payload['collector_mode']) && $payload['collector_mode'] === 'dependency-blocked');
}
