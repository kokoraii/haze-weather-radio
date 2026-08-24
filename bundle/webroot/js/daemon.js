import { panelClient } from './lib/ws-client.js';

const formEl = document.getElementById('daemonSettingsForm');
const runtimeList = document.getElementById('daemonRuntimeList');
const statusBanner = document.getElementById('daemonStatusBanner');
const settingsPathEl = document.getElementById('daemonSettingsPath');
const settingsStateEl = document.getElementById('daemonSettingsState');
const goStateEl = document.getElementById('daemonGoState');
const saveButton = document.getElementById('daemonSaveButton');
const reloadButton = document.getElementById('daemonReloadButton');

let bound = false;
let currentSettings = null;

const SECTIONS = [
    {
        title: 'Go Services',
        items: [
            ['services.go.enabled', 'Enable service host', 'Allow Haze to extract and run bundled Go services.'],
            ['services.go.web_gateway.enabled', 'Web gateway', 'Serve the public/admin gateway process.'],
            ['services.go.data_ingest.enabled', 'Weather data ingest', 'Refresh routine weather data caches.'],
            ['services.go.tts.enabled', 'TTS renderer', 'Render speech audio through configured SAPI5, SpeakyAPI, eSpeak, or native readers.'],
            ['services.go.product_render.enabled', 'Product renderer', 'Build feed-aware TTS products for the dynamic playlist.'],
            ['services.go.playlist.enabled', 'Dynamic playlist', 'Predict, queue, and schedule product playback per feed.'],
            ['services.go.ivr.enabled', 'Phone IVR edge', 'Serve cache-first weather audio for SIP and provider-webhook telephone access.'],
        ],
        selects: [
            ['services.go.ivr.mode', 'IVR mode', [
                ['sip-edge', 'SIP edge'],
                ['provider-webhook', 'Provider webhook'],
            ]],
        ],
        fields: [
            ['services.go.data_ingest.interval', 'Weather data interval', '45m'],
            ['services.go.data_ingest.timeout', 'Weather data timeout', '20s'],
            ['services.go.tts.readers', 'Reader config', 'managed/configs/readers.xml'],
            ['services.go.tts.provider', 'Default TTS provider', 'auto'],
            ['services.go.tts.language', 'Default TTS language', 'en-CA'],
            ['services.go.tts.out_dir', 'TTS output directory', 'runtime/audio/tts'],
            ['services.go.tts.timeout', 'TTS timeout', '60s'],
            ['services.go.tts.piper_voices_dir', 'Piper voices directory', 'managed/voices/piper'],
            ['services.go.tts.speakyapi_url', 'SpeakyAPI URL', ''],
            ['services.go.product_render.refresh', 'Product config refresh', '5m'],
            ['services.go.playlist.tick', 'Playlist tick', '500ms'],
            ['services.go.playlist.lookahead', 'Playlist lookahead', '2m'],
            ['services.go.playlist.max_queued', 'Playlist max queued', '3'],
            ['services.go.playlist.out_dir', 'Playlist audio directory', 'runtime/audio/playlist'],
            ['services.go.playlist.fixed_tolerance_s', 'Fixed event tolerance s', '4'],
            ['services.go.playlist.routine_estimate_s', 'Routine estimate s', '35'],
            ['services.go.ivr.http.addr', 'IVR webhook address', '127.0.0.1:8096'],
            ['services.go.ivr.sip.listen', 'IVR SIP listen', '0.0.0.0:5060'],
            ['services.go.ivr.sip.public_host', 'IVR SIP public host', 'radio.example.com'],
            ['services.go.ivr.cache.dir', 'IVR cache directory', 'runtime/ivr/cache'],
            ['services.go.ivr.cache.ttl', 'IVR cache TTL', '10m'],
            ['services.go.ivr.cache.phone_sample_rate', 'IVR phone sample rate', '8000'],
            ['services.go.ivr.cache.phone_codec', 'IVR phone codec', 'pcmu'],
            ['services.go.ivr.cache.max_entries', 'IVR max cache entries', '5000'],
            ['services.go.ivr.max_concurrent_calls', 'IVR max calls', '256'],
            ['services.go.ivr.max_render_inflight', 'IVR render slots', '8'],
        ],
    },
    {
        title: 'Rust CAP Ingest',
        items: [
            ['services.rust.cap_ingest.enabled', 'CAP ingest', 'Run the native CAP ingest service.'],
            ['services.rust.cap_ingest.shadow', 'Shadow only', 'Publish diagnostics without relaying CAP alert events.'],
            ['services.rust.cap_ingest.startup_seed', 'Seed on startup', 'Suppress replay of already-current provider entries on startup.'],
        ],
        selects: [
            ['services.rust.cap_ingest.source', 'CAP source', [
                ['naads', 'NAADS / CAP-CP'],
                ['nws', 'NWS CAP'],
                ['custom', 'Custom'],
            ]],
            ['services.rust.cap_ingest.mode', 'Ingest mode', [
                ['tcp', 'TCP streaming'],
                ['atom', 'Atom polling'],
                ['auto', 'Auto'],
            ]],
        ],
        fields: [
            ['services.rust.cap_ingest.source_id', 'CAP source ID', 'rust-cap'],
            ['services.rust.cap_ingest.url', 'Primary URL', 'tcp://streaming1.naad-adna.pelmorex.com:8080'],
            ['services.rust.cap_ingest.fallback_url', 'Fallback URL(s)', 'tcp://streaming2.naad-adna.pelmorex.com:8080'],
            ['services.rust.cap_ingest.archive_url', 'Primary archive URL', 'http://capcp1.naad-adna.pelmorex.com'],
            ['services.rust.cap_ingest.fallback_archive_url', 'Fallback archive URL', 'http://capcp2.naad-adna.pelmorex.com'],
            ['services.rust.cap_ingest.interval', 'Atom poll interval', '5s'],
            ['services.rust.cap_ingest.timeout', 'HTTP timeout', '15s'],
            ['services.rust.cap_ingest.concurrency', 'CAP fetch concurrency', '8'],
        ],
    },
    {
        title: 'Host Services',
        items: [
            ['services.daemon.enabled', 'Enable host service loop', 'Allow Haze to run scheduler and playlist services.'],
            ['services.daemon.scheduler.enabled', 'Host scheduler', 'Emit station ID and date/time schedule events over the host bridge.'],
            ['services.daemon.alert_queue.enabled', 'Alert queue worker', 'Claim and time queued SAME alert audio from managed queues.'],
		['services.daemon.alert_update_tone_policy', 'Tone Updates', 'Control whether SAME tones are played for updated alerts: all, none, or only for new locations.'],
            ['services.daemon.playlist.enabled', 'Host playlist', 'Drive queue refill ticks over the host bridge.'],
        ],
        fields: [
            ['services.daemon.alert_queue.interval_ms', 'Alert queue poll ms', '500'],
            ['services.daemon.playlist.interval_ms', 'Playlist tick ms', '750'],
        ],
    },
    {
        title: 'Web And Receiver',
        items: [
            ['webpanel.public.enabled', 'Public site', 'Serve the public status site.'],
            ['webpanel.public.feeds.webrtc.enabled', 'Public WebRTC', 'Allow direct public feed playback when feed access permits it.'],
            ['webpanel.admin.enabled', 'Admin panel', 'Serve the protected operator panel.'],
            ['webpanel.tls.enabled', 'HTTPS / ACME', 'Serve the web panel with TLS. ACME uses Let\'s Encrypt with approved domains.'],
            ['webpanel.tls.redirect_http', 'Redirect HTTP', 'Redirect normal HTTP requests on the challenge listener to HTTPS.'],
            ['webpanel.tls.hsts', 'HSTS header', 'Only enable after the domain works reliably over HTTPS.'],
            ['webpanel.tls.staging', 'ACME staging', 'Use the Let\'s Encrypt staging directory while testing.'],
            ['webpanel.tls.http_challenge.enabled', 'ACME HTTP challenge', 'Serve Let\'s Encrypt HTTP-01 challenges on the configured challenge listener.'],
            ['webpanel.receiver.enabled', 'Haze receiver pairing', 'Allow secure receiver pairing and WebRTC feed transport.'],
        ],
        selects: [
            ['webpanel.public.feeds.access', 'Public feed access', [
                ['disabled', 'Disabled'],
                ['public', 'Public'],
                ['auth_required', 'Auth required'],
            ]],
            ['webpanel.tls.mode', 'TLS mode', [
                ['acme', 'Let\'s Encrypt ACME'],
                ['manual', 'Manual cert files'],
            ]],
        ],
        fields: [
            ['webpanel.public.host', 'Public host', '0.0.0.0'],
            ['webpanel.public.port', 'Public port', '6444'],
            ['webpanel.admin.host', 'Admin host', '0.0.0.0'],
            ['webpanel.admin.port', 'Admin port', '6444'],
            ['webpanel.tls.domains', 'ACME domains', 'radio.example.com'],
            ['webpanel.tls.email', 'ACME email', 'operator@example.com'],
            ['webpanel.tls.cache_dir', 'ACME cache', 'runtime/tls/acme'],
            ['webpanel.tls.cert_file', 'Manual cert file', 'managed/certs/fullchain.pem'],
            ['webpanel.tls.key_file', 'Manual key file', 'managed/certs/privkey.pem'],
            ['webpanel.tls.http_challenge.host', 'Challenge host', '0.0.0.0'],
            ['webpanel.tls.http_challenge.port', 'Challenge port (external 80)', '80'],
        ],
    },
    {
        title: 'Optional Workloads',
        items: [
            ['cap.cap_cp.enabled', 'CAP-CP alerts', 'Enable Canadian CAP-CP alert processing.'],
            ['cap.nws_cap.enabled', 'NWS CAP alerts', 'Enable US NWS CAP alert processing.'],
            ['wx_on_demand.enabled', 'WX on-demand', 'Enable on-demand weather package generation.'],
            ['playout.station_id_schedule.enabled', 'Station ID schedule', 'Air station IDs on schedule.'],
            ['playout.date_time_schedule.enabled', 'Date/time schedule', 'Air date/time packages on schedule.'],
            ['playout.chimes.enabled', 'Chimes', 'Air configured chime sequences.'],
        ],
    },
];

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[char]));
}

function getPath(object, dotted, fallback = undefined) {
    return dotted.split('.').reduce((value, key) => (
        value && typeof value === 'object' && key in value ? value[key] : fallback
    ), object);
}

function setPath(object, dotted, value) {
    const parts = dotted.split('.');
    let cursor = object;
    parts.slice(0, -1).forEach((part) => {
        cursor[part] = cursor[part] && typeof cursor[part] === 'object' ? cursor[part] : {};
        cursor = cursor[part];
    });
    cursor[parts[parts.length - 1]] = value;
}

function renderToggle(path, label, description) {
    const checked = getPath(currentSettings, path, false);
    return `
        <label class="daemon-toggle" title="${escapeHtml(description)}">
            <input type="checkbox" data-setting="${escapeHtml(path)}" ${checked ? 'checked' : ''}>
            <span class="daemon-toggle-ui"></span>
            <span>
                <strong>${escapeHtml(label)}</strong>
                <small>${escapeHtml(description)}</small>
            </span>
        </label>
    `;
}

function renderField(path, label, placeholder) {
    const rawValue = getPath(currentSettings, path, '');
    const value = Array.isArray(rawValue) ? rawValue.join(', ') : rawValue;
    return `
        <label class="daemon-field">
            <span>${escapeHtml(label)}</span>
            <input data-setting="${escapeHtml(path)}" type="text" value="${escapeHtml(value)}" placeholder="${escapeHtml(placeholder)}">
        </label>
    `;
}

function coerceFieldValue(path, raw) {
    if (path === 'webpanel.tls.domains') {
        return raw.split(/[\s,;]+/).map((item) => item.trim()).filter(Boolean);
    }
    if (/(\.port|_ms|_seconds)$/.test(path) && /^\d+$/.test(raw)) {
        return Number(raw);
    }
    return raw;
}

function renderSelect(path, label, options) {
    const value = getPath(currentSettings, path, '');
    const optionHtml = options.map(([optionValue, text]) => `
        <option value="${escapeHtml(optionValue)}" ${optionValue === value ? 'selected' : ''}>${escapeHtml(text)}</option>
    `).join('');
    return `
        <label class="daemon-field">
            <span>${escapeHtml(label)}</span>
            <select data-setting="${escapeHtml(path)}">${optionHtml}</select>
        </label>
    `;
}

function renderSettings(payload) {
    if (!payload?.effective || typeof payload.effective !== 'object') {
        return;
    }
    currentSettings = structuredClone(payload?.effective || {});
    if (!currentSettings.services) currentSettings.services = {};

    formEl.innerHTML = SECTIONS.map((section) => `
        <article class="daemon-group">
            <h2>${escapeHtml(section.title)}</h2>
            <div class="daemon-toggle-list">
                ${(section.items || []).map(([path, label, description]) => renderToggle(path, label, description)).join('')}
            </div>
            ${section.fields?.length ? `<div class="daemon-field-grid">${section.fields.map(([path, label, placeholder]) => renderField(path, label, placeholder)).join('')}</div>` : ''}
            ${section.selects?.length ? `<div class="daemon-field-grid">${section.selects.map(([path, label, options]) => renderSelect(path, label, options)).join('')}</div>` : ''}
        </article>
    `).join('');

    settingsPathEl.textContent = payload?.settings_path || 'runtime/state/daemonSettings.json';
    renderRuntime(payload?.go_runtime);
    updateMetrics(Boolean(payload?.pending_restart));
    window.lucide?.createIcons();
}

function renderRuntime(runtime) {
    const services = runtime?.services && typeof runtime.services === 'object' ? Object.values(runtime.services) : [];
    if (!services.length) {
        runtimeList.innerHTML = '<article class="daemon-runtime-item empty">No managed services reported by the host.</article>';
        return;
    }
    runtimeList.innerHTML = services.map((service) => `
        <article class="daemon-runtime-item">
            <div class="daemon-runtime-main">
                <strong>${escapeHtml(service.id || 'managed service')}</strong>
                <span>${escapeHtml(runtimeDetail(service))}</span>
            </div>
            <div class="daemon-runtime-controls">
                <span class="daemon-status" data-state="${escapeHtml(service.status || 'unknown')}">${escapeHtml(service.status || 'unknown')}</span>
                <button class="btn-ghost daemon-service-btn" type="button" title="Start service" aria-label="Start ${escapeHtml(service.id || 'service')}" data-service-action="start" data-service-id="${escapeHtml(service.id || '')}" ${serviceRunning(service) ? 'disabled' : ''}>
                    <i data-lucide="play" width="12" height="12"></i>
                </button>
                <button class="btn-ghost daemon-service-btn" type="button" title="Restart service" aria-label="Restart ${escapeHtml(service.id || 'service')}" data-service-action="restart" data-service-id="${escapeHtml(service.id || '')}">
                    <i data-lucide="rotate-cw" width="12" height="12"></i>
                </button>
                <button class="btn-ghost daemon-service-btn" type="button" title="Stop service" aria-label="Stop ${escapeHtml(service.id || 'service')}" data-service-action="stop" data-service-id="${escapeHtml(service.id || '')}" ${serviceStopped(service) ? 'disabled' : ''}>
                    <i data-lucide="square" width="12" height="12"></i>
                </button>
            </div>
        </article>
    `).join('');
    window.lucide?.createIcons();
}

function runtimeDetail(service) {
    const parts = [];
    if (service.pid) parts.push(`pid ${service.pid}`);
    if (service.restart_count) parts.push(`${service.restart_count} restarts`);
    if (service.desired && service.desired !== service.status) parts.push(`desired ${service.desired}`);
    if (parts.length) return parts.join(' | ');
    return service.executable || service.last_error || 'no executable detail';
}

function serviceRunning(service) {
    return ['running', 'restarting'].includes(String(service.status || '').toLowerCase());
}

function serviceStopped(service) {
    return ['stopped', 'missing'].includes(String(service.status || '').toLowerCase());
}

async function controlService(serviceID, action, button) {
    if (!serviceID || !action) return;
    const previous = statusBanner.textContent;
    button.disabled = true;
    statusBanner.textContent = `${action[0].toUpperCase()}${action.slice(1)} requested for ${serviceID}.`;
    statusBanner.dataset.state = 'pending';
    try {
        await panelClient.command('daemon.service.control', { service_id: serviceID, action }, 5000);
        await new Promise((resolve) => setTimeout(resolve, 500));
        await loadSettings();
        statusBanner.textContent = `${action[0].toUpperCase()}${action.slice(1)} requested for ${serviceID}.`;
        statusBanner.dataset.state = 'ok';
    } catch (error) {
        statusBanner.textContent = error.message || previous || 'Service control failed.';
        statusBanner.dataset.state = 'err';
        button.disabled = false;
    }
}

function updateMetrics(pendingRestart = false) {
    const goEnabled = Boolean(getPath(currentSettings, 'services.go.enabled', false));
    const goWeb = Boolean(getPath(currentSettings, 'services.go.web_gateway.enabled', false));
    const goData = Boolean(getPath(currentSettings, 'services.go.data_ingest.enabled', false));
    const goTts = Boolean(getPath(currentSettings, 'services.go.tts.enabled', false));
    const goProduct = Boolean(getPath(currentSettings, 'services.go.product_render.enabled', false));
    const goPlaylist = Boolean(getPath(currentSettings, 'services.go.playlist.enabled', false));
    const goIvr = Boolean(getPath(currentSettings, 'services.go.ivr.enabled', false));
    const rustCap = Boolean(getPath(currentSettings, 'services.rust.cap_ingest.enabled', false));
    const hostEnabled = Boolean(getPath(currentSettings, 'services.daemon.enabled', false));
    const hostScheduler = Boolean(getPath(currentSettings, 'services.daemon.scheduler.enabled', false));
    const hostAlerts = Boolean(getPath(currentSettings, 'services.daemon.alert_queue.enabled', false));
    const hostPlaylist = Boolean(getPath(currentSettings, 'services.daemon.playlist.enabled', false));
    settingsStateEl.textContent = pendingRestart ? 'pending restart' : 'loaded';
    const goText = goEnabled ? `${[goWeb && 'web', goData && 'data', goTts && 'tts', goProduct && 'products', goPlaylist && 'playlist', goIvr && 'ivr'].filter(Boolean).join(' + ') || 'service host'}` : '';
    const rustText = rustCap ? 'rust cap' : '';
    const hostText = hostEnabled ? `${[hostScheduler && 'sched', hostAlerts && 'alerts', hostPlaylist && 'playlist'].filter(Boolean).join(' + ') || 'host loop'}` : '';
    goStateEl.textContent = [goText, rustText, hostText].filter(Boolean).join(' / ') || 'disabled';
}

function collectSettings() {
    const next = structuredClone(currentSettings || {});
    formEl.querySelectorAll('[data-setting]').forEach((input) => {
        if (input.type === 'checkbox') {
            setPath(next, input.dataset.setting, input.checked);
        } else {
            setPath(next, input.dataset.setting, coerceFieldValue(input.dataset.setting, input.value.trim()));
        }
    });
    return next;
}

async function loadSettings() {
    statusBanner.textContent = 'Loading daemon settings…';
    statusBanner.dataset.state = 'pending';
    const payload = await panelClient.command('daemon.settings.get', {}, 8000);
    renderSettings(payload);
    statusBanner.textContent = 'Daemon settings loaded. Service changes apply after restarting Haze.';
    statusBanner.dataset.state = 'ok';
}

async function saveSettings() {
    saveButton.disabled = true;
    statusBanner.textContent = 'Saving daemon settings…';
    statusBanner.dataset.state = 'pending';
    try {
        const payload = await panelClient.command('daemon.settings.save', { settings: collectSettings() }, 12000);
        renderSettings(payload);
        statusBanner.textContent = 'Saved. Restart Haze to apply service and listener changes.';
        statusBanner.dataset.state = 'warn';
    } catch (error) {
        statusBanner.textContent = error.message || 'Save failed.';
        statusBanner.dataset.state = 'err';
    } finally {
        saveButton.disabled = false;
    }
}

export function initDaemonView() {
    if (bound) {
        loadSettings().catch((error) => {
            statusBanner.textContent = error.message || 'Unable to load daemon settings.';
            statusBanner.dataset.state = 'err';
        });
        return;
    }
    bound = true;
    saveButton.addEventListener('click', saveSettings);
    reloadButton.addEventListener('click', () => loadSettings().catch((error) => {
        statusBanner.textContent = error.message || 'Unable to reload daemon settings.';
        statusBanner.dataset.state = 'err';
    }));
    formEl.addEventListener('change', () => {
        currentSettings = collectSettings();
        updateMetrics();
    });
    runtimeList.addEventListener('click', (event) => {
        const button = event.target.closest('[data-service-action]');
        if (!button) return;
        controlService(button.dataset.serviceId, button.dataset.serviceAction, button).catch((error) => {
            statusBanner.textContent = error.message || 'Service control failed.';
            statusBanner.dataset.state = 'err';
        });
    });
    window.addEventListener('haze:admin-state', (event) => {
        if (event.detail?.daemon?.effective) {
            renderSettings(event.detail.daemon);
        }
    });
    loadSettings().catch((error) => {
        statusBanner.textContent = error.message || 'Unable to load daemon settings.';
        statusBanner.dataset.state = 'err';
    });
}
