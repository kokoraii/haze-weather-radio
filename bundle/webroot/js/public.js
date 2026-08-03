import { PanelClient } from './lib/ws-client.js';
import { session } from './lib/session.js';
import { initTheme } from './lib/theme.js';

const API_BASE = '/api/public/v1';
const PUBLIC_BASEMAP_PROFILE_URL = `${API_BASE}/map/basemap`;

const summaryCards = document.getElementById('publicSummaryCards');
const buildInfo = document.getElementById('publicBuildInfo');
const feedsGrid = document.getElementById('publicFeedsGrid');
const adminLink = document.getElementById('adminLink');
const adminHint = document.getElementById('adminHint');
const feedNotice = document.getElementById('publicFeedNotice');
const alertsNotice = document.getElementById('publicAlertsNotice');
const alertsList = document.getElementById('publicAlertsList');
const alertsLink = document.getElementById('publicAlertsLink');
const publicSummarySection = document.getElementById('publicSummary');
const publicSiteTitle = document.getElementById('publicSiteTitle');
const publicFeedsSection = document.getElementById('publicFeedsSection');
const publicListenSection = document.getElementById('publicListenSection');
const listenNotice = document.getElementById('publicListenNotice');
const listenPanel = document.getElementById('publicListenPanel');
const publicAlertsSection = document.getElementById('publicAlertsSection');
const publicTlsNotice = document.getElementById('publicTlsNotice');
const publicTlsNoticeText = document.getElementById('publicTlsNoticeText');
const themeToggle = document.getElementById('themeToggle');

initTheme(themeToggle);

let summaryState = null;
const currentPath = window.location.pathname.replace(/\/+$/, '') || '/';
const currentPage = currentPath === '/feeds' || currentPath === '/listen'
    ? 'feeds'
    : (currentPath === '/alerts' || currentPath === '/alerts/archive' ? 'alerts' : 'home');
const isListenPage = currentPath === '/listen';
if (currentPage === 'feeds') {
    session.importUrlToken();
} else {
    removeTokenFromPublicURL();
}
const publicClient = new PanelClient({
    base: API_BASE,
    stream: true,
    params: currentPage === 'feeds' ? { feeds: '1' } : (currentPage === 'alerts' ? { alerts: '1' } : {}),
    includeSessionToken: false,
});
let lastSummarySignature = '';
let lastFeedsSignature = '';
let lastAlertsSignature = '';
let lastListenSignature = '';
let lastNoticeText = '';
let activeAlertTab = 'accepted';
let publicFeedTokenReconnectAttempted = false;
let listenMap = null;
let listenMapPopup = null;
let listenMapAbort = null;
let listenAlertAbort = null;
let listenAlertReloadTimer = null;

function removeTokenFromPublicURL() {
    try {
        const params = new URLSearchParams(window.location.search);
        if (!params.has('token')) return;
        params.delete('token');
        const cleanSearch = params.toString();
        const cleanUrl = `${window.location.pathname}${cleanSearch ? `?${cleanSearch}` : ''}${window.location.hash || ''}`;
        window.history.replaceState(null, '', cleanUrl);
    } catch {
        // If history is unavailable, the public client still avoids forwarding the token.
    }
}
const feedPlayers = new Map();
const feedPreferences = new Map();
const feedCodecFallbacks = new Map();
const feedReconnectBackoffs = new Map();
const httpReconnectTimers = new Map();
const httpReconnectAttempts = new Map();
const publicFeedRuntimeByID = new Map();
let publicFeedRuntimeTimer = null;
const HTTP_RECONNECT_BASE_MS = 500;
const HTTP_RECONNECT_MAX_MS = 8000;
const HTTP_STALL_RECONNECT_MS = 7000;
window.hazeFeedPlayers = feedPlayers;

function recordWebRTCEvent(feedId, event, details = {}) {
    const events = window.hazeWebRTCEvents || [];
    events.push({
        feed_id: String(feedId || ''),
        event,
        ...details,
        at: new Date().toISOString(),
    });
    window.hazeWebRTCEvents = events.slice(-80);
}

window.hazeDumpWebRTC = function hazeDumpWebRTC(feedId = '') {
    const requestedFeed = String(feedId || '').trim();
    const players = {};
    for (const [id, player] of feedPlayers.entries()) {
        if (requestedFeed && id !== requestedFeed) continue;
        const audio = player.audio || null;
        const codecFallback = feedCodecFallbacks.get(id) || null;
        const now = Date.now();
        players[id] = {
            mode: player.mode || '',
            connection_state: player.pc?.connectionState || '',
            ice_state: player.pc?.iceConnectionState || '',
            signaling_state: player.pc?.signalingState || '',
            track_attached: Boolean(player.trackAttached),
            has_live_track: hasLiveWebRTCAudioTrack(player),
            last_packet_age_ms: player.lastPacketAt ? Date.now() - player.lastPacketAt : null,
            last_audio_progress_age_ms: player.lastAudioProgressAt ? Date.now() - player.lastAudioProgressAt : null,
            last_playback_progress_age_ms: player.lastPlaybackProgressAt ? now - player.lastPlaybackProgressAt : null,
            packet_source_age_ms: webRTCPacketSourceAgeMS(player),
            packets_recent: hasRecentWebRTCPackets(player),
            playback_watchdog_active: Boolean(player.playbackWatchdogTimer),
            stagnant_stats_polls: Number(player.stagnantStatsPolls || 0),
            missing_stats_polls: Number(player.missingStatsPolls || 0),
            reconnect_pending: Boolean(player.reconnectPending || player.reconnectTimer),
            disconnect_reconnect_pending: Boolean(player.disconnectReconnectTimer),
            media_recent: player.mediaRecent,
            requested_codec: player.requestedCodec || 'auto',
            negotiated_codec: player.negotiatedCodec || '',
            negotiated_payload_type: player.negotiatedPayloadType,
            codec_fallback_applied: Boolean(player.codecFallbackApplied || codecFallback),
            codec_fallback: codecFallback,
            track_mute_pending: Boolean(player.trackMuteTimer),
            track_muted_reported: Boolean(player.trackMutedReported),
            track_mute_signals: Number(player.trackMuteSignalCount || 0),
            track_unmute_signals: Number(player.trackUnmuteSignalCount || 0),
            last_track_mute_signal_age_ms: player.lastTrackMuteSignalAt ? now - player.lastTrackMuteSignalAt : null,
            last_track_unmute_signal_age_ms: player.lastTrackUnmuteSignalAt ? now - player.lastTrackUnmuteSignalAt : null,
            track_states: webRTCAudioTrackStates(player, audio),
            output_mixer_active: Boolean(player.audioOutputMixer),
            output_mixer_state: player.audioOutputMixer?.context?.state || '',
            output_mixer_disabled: webRTCOutputMixerDisabled(),
            last_output_mixer_recovery_age_ms: player.lastOutputMixerRecoveryAt ? now - player.lastOutputMixerRecoveryAt : null,
            output_mixer_recovery_count: Number(player.outputMixerRecoveryCount || 0),
            output_mixer_bypass_remaining_ms: player.outputMixerBypassUntil && player.outputMixerBypassUntil > now ? player.outputMixerBypassUntil - now : null,
            audio: audio ? {
                paused: audio.paused,
                ended: audio.ended,
                ready_state: audio.readyState,
                network_state: audio.networkState,
                current_time: Number(audio.currentTime || 0),
                muted: audio.muted,
                volume: audio.volume,
                player_state: audio.dataset.hazePlayerState || '',
                track_attached: audio.dataset.hazeTrackAttached || '',
                stream_attached: audio.dataset.hazeStreamAttached || '',
                stream_track_count: audio.srcObject?.getAudioTracks?.().length || 0,
            } : null,
        };
    }
    const stats = window.hazeLastWebRTCStats || {};
    return {
        at: new Date().toISOString(),
        requested_feed: requestedFeed,
        players,
        stats: requestedFeed ? { [requestedFeed]: stats[requestedFeed] || null } : stats,
        events: (window.hazeWebRTCEvents || [])
            .filter((event) => !requestedFeed || event.feed_id === requestedFeed)
            .slice(-30),
    };
};

const WEBRTC_CODECS = [
    ['auto', 'Auto'],
    ['opus', 'Opus'],
    ['g722', 'G.722'],
    ['pcmu', 'PCMU'],
    ['pcma', 'PCMA'],
];
const DEFAULT_FEED_MODE = 'http';
const DEFAULT_WEBRTC_CODEC = 'auto';
const DEFAULT_HTTP_CODEC = 'opus';
const HTTP_CODECS = [
    ['opus', 'Opus / Ogg'],
    ['pcm16', 'PCM16 / WAV'],
    ['webm_opus', 'Opus / WebM'],
    ['aac', 'AAC / ADTS'],
    ['m4a', 'AAC / fragmented MP4'],
    ['mp3', 'MP3'],
    ['vorbis', 'Vorbis / Ogg'],
    ['flac', 'FLAC'],
    ['ogg_flac', 'FLAC / Ogg'],
    ['ulaw', 'G.711 u-law'],
    ['alaw', 'G.711 A-law'],
    ['raw_pcm16', 'Raw PCM16'],
];
const HTTP_CODEC_VALUES = new Set(HTTP_CODECS.map(([value]) => value));
const PUBLIC_FEED_PREF_COOKIE = 'haze_public_feed_prefs';
let publicFeedGlobalPreferences = readPublicFeedGlobalPreferences();
let currentPublicFeeds = [];
const WEBRTC_TRANSIENT_STATUS_DELAY_MS = 2000;
const WEBRTC_STATS_INTERVAL_MS = 2000;
const WEBRTC_STAGNANT_STATS_POLLS = 3;
const WEBRTC_RECOVER_STATS_POLLS = 15;
const WEBRTC_RECONNECT_BASE_DELAY_MS = 1000;
const WEBRTC_RECONNECT_MAX_DELAY_MS = 10000;
const WEBRTC_DISCONNECT_GRACE_MS = 15000;
const WEBRTC_MEDIA_EVENT_GRACE_MS = 12000;
const WEBRTC_RECENT_PACKET_GRACE_MS = 30000;
const WEBRTC_HARD_PACKET_STALE_MS = 120000;
const WEBRTC_PLAYBACK_WATCHDOG_MS = 2500;
const WEBRTC_TRACK_MUTE_GRACE_MS = 2500;
const WEBRTC_OUTPUT_BED_GAIN = 0.0025;
const WEBRTC_OUTPUT_BED_FREQUENCY = 180;
const WEBRTC_OUTPUT_MIXER_RESUME_GRACE_MS = 1500;
const WEBRTC_OUTPUT_MIXER_RECOVERY_COOLDOWN_MS = 10000;
const WEBRTC_OUTPUT_MIXER_RECOVERY_WINDOW_MS = 60000;
const WEBRTC_OUTPUT_MIXER_RECOVERY_LIMIT = 3;
const WEBRTC_OUTPUT_MIXER_BYPASS_MS = 60000;

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[char]));
}

function publicText(value, fallback = '') {
    if (value == null) return fallback;
    if (Array.isArray(value)) {
        const text = value.map((item) => publicText(item)).filter(Boolean).join('; ');
        return text || fallback;
    }
    if (typeof value === 'object') {
        for (const key of ['name', 'area_name', 'area', 'area_text', 'description', 'headline', 'event', 'label', 'text', 'value', 'id']) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const text = publicText(value[key]);
                if (text) return text;
            }
        }
        return fallback;
    }
    const text = String(value).trim();
    return text || fallback;
}

function publicNowPlaying(value) {
    const text = publicText(value, '').trim();
    if (!text || text.toLowerCase() === 'idle') {
        return 'Current Conditions';
    }
    return text;
}

function readPublicFeedGlobalPreferences() {
    let stored = {};
    try {
        const prefix = `${PUBLIC_FEED_PREF_COOKIE}=`;
        const cookie = document.cookie
            .split(';')
            .map((item) => item.trim())
            .find((item) => item.startsWith(prefix));
        if (cookie) {
            stored = JSON.parse(decodeURIComponent(cookie.slice(prefix.length)));
        }
    } catch {
        stored = {};
    }
    const mode = stored.mode === 'webrtc' ? 'webrtc' : DEFAULT_FEED_MODE;
    const codec = mode === 'http'
        ? normalizeHTTPCodec(stored.codec || DEFAULT_HTTP_CODEC)
        : (WEBRTC_CODECS.some(([value]) => value === stored.codec) ? stored.codec : DEFAULT_WEBRTC_CODEC);
    return { mode, codec };
}

function writePublicFeedGlobalPreferences() {
    const prefs = normalizePublicFeedGlobalPreferences();
    const encoded = encodeURIComponent(JSON.stringify(prefs));
    document.cookie = `${PUBLIC_FEED_PREF_COOKIE}=${encoded}; Max-Age=31536000; Path=/; SameSite=Lax`;
}

function normalizePublicFeedGlobalPreferences(feed = null) {
    const canWebRTC = feed ? feedCanWebRTC(feed) : currentPublicFeeds.some(feedCanWebRTC);
    const canHTTP = feed ? feedCanHTTP(feed) : currentPublicFeeds.some(feedCanHTTP);
    let mode = publicFeedGlobalPreferences.mode === 'webrtc' ? 'webrtc' : 'http';
    if (mode === 'webrtc' && !canWebRTC && canHTTP) mode = 'http';
    if (mode === 'http' && !canHTTP && canWebRTC) mode = 'webrtc';
    const allowedCodecs = codecOptionsForMode(mode).map(([value]) => value);
    let codec = allowedCodecs.includes(publicFeedGlobalPreferences.codec)
        ? publicFeedGlobalPreferences.codec
        : defaultCodecForMode(mode);
    if (!allowedCodecs.includes(codec)) {
        codec = allowedCodecs[0];
    }
    return { mode, codec };
}

function publicRuntimeTitle(runtime) {
    return publicNowPlaying(
        runtime?.now_playing
        || runtime?.public_stream_now_playing
        || runtime?.on_air_now_playing
        || runtime?.station_now_playing
    );
}

function parsePublicRuntimeTime(value) {
    if (!value) return null;
    const time = new Date(value).getTime();
    return Number.isFinite(time) ? time : null;
}

function publicRuntimeRemainingMs(runtime, now = Date.now()) {
    if (!runtime || typeof runtime !== 'object') return null;
    const end = parsePublicRuntimeTime(runtime.current_ends_at || runtime.public_stream_ends_at || runtime.expected_end_at);
    if (end != null) {
        return Math.max(0, end - now);
    }
    const remaining = Number(runtime.current_remaining_ms ?? runtime.remaining_ms ?? runtime.product_remaining_ms);
    return Number.isFinite(remaining) && remaining >= 0 ? remaining : null;
}

function formatPublicRuntimeRemaining(ms) {
    if (!Number.isFinite(ms)) return '';
    const seconds = Math.max(0, Math.ceil(ms / 1000));
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainder = seconds % 60;
    if (hours > 0) {
        return `next in ${hours}h ${minutes}m`;
    }
    if (minutes > 0) {
        return `next in ${minutes}m ${remainder}s`;
    }
    return `next in ${remainder}s`;
}

function publicRuntimeRemainingText(runtime) {
    const remaining = publicRuntimeRemainingMs(runtime);
    return remaining == null ? '' : formatPublicRuntimeRemaining(remaining);
}

function listenerCountLabel(count) {
    const value = Math.max(0, Number(count) || 0);
    return String(value);
}

function peakListenerLabel(count) {
    const value = Math.max(0, Number(count) || 0);
    return String(value);
}

function peakListenerTitle(listeners) {
    const label = 'Peak concurrent listeners';
    const peakAt = listeners?.peak_at || listeners?.peakAt || '';
    if (!peakAt) return label;
    const date = new Date(peakAt);
    if (Number.isNaN(date.getTime())) return `${label}\nRecorded ${peakAt}`;
    return `${label}\nRecorded ${date.toLocaleString()}`;
}

function setPublicFeedListeners(feedId, listeners) {
    feedId = String(feedId || '');
    if (!feedId) return;
    const current = findFeedElement('feed-listener-current', feedId);
    const currentMetric = findFeedElement('feed-listener-current-metric', feedId);
    const peak = findFeedElement('feed-listener-peak', feedId);
    const peakMetric = findFeedElement('feed-listener-peak-metric', feedId);
    const payload = listeners && typeof listeners === 'object' ? listeners : {};
    if (current) {
        setTextIfChanged(current, listenerCountLabel(payload.current));
    }
    if (currentMetric) {
        currentMetric.title = 'Concurrent listeners';
        currentMetric.setAttribute('aria-label', `Concurrent listeners: ${listenerCountLabel(payload.current)}`);
    }
    if (peak) {
        setTextIfChanged(peak, peakListenerLabel(payload.peak));
    }
    if (peakMetric) {
        peakMetric.title = peakListenerTitle(payload);
        peakMetric.setAttribute('aria-label', `Peak concurrent listeners: ${peakListenerLabel(payload.peak)}`);
    }
}

function setPublicFeedRuntime(feedId, runtime) {
    feedId = String(feedId || '');
    if (!feedId) return;
    publicFeedRuntimeByID.set(feedId, runtime && typeof runtime === 'object' ? runtime : {});
    updatePublicFeedRuntimeLine(feedId);
    ensurePublicRuntimeTimer();
}

function updatePublicFeedRuntimeLine(feedId) {
    const runtime = publicFeedRuntimeByID.get(String(feedId || '')) || {};
    const title = publicRuntimeTitle(runtime);
    const titleElement = findFeedElement('feed-now-title', feedId) || findFeedElement('feed-now', feedId);
    setTextIfChanged(titleElement, title);
    const remainingElement = findFeedElement('feed-remaining', feedId);
    if (remainingElement) {
        const remainingMs = publicRuntimeRemainingMs(runtime);
        const remaining = remainingMs == null ? '' : formatPublicRuntimeRemaining(remainingMs);
        remainingElement.hidden = !remaining;
        remainingElement.classList.toggle('is-expired', remainingMs != null && remainingMs <= 0);
        setTextIfChanged(remainingElement, remaining);
    }
}

function ensurePublicRuntimeTimer() {
    if (publicFeedRuntimeTimer || currentPage !== 'feeds') return;
    publicFeedRuntimeTimer = window.setInterval(updatePublicRuntimeCountdowns, 1000);
}

function updatePublicRuntimeCountdowns() {
    for (const feedId of publicFeedRuntimeByID.keys()) {
        updatePublicFeedRuntimeLine(feedId);
    }
}

function safePublicLink(value, { allowRelative = false, allowHTTP = true } = {}) {
    const raw = String(value || '').trim();
    if (!raw) return '';
    try {
        const parsed = new URL(raw, window.location.origin);
        const isSameOriginRelative = allowRelative && !/^[a-z][a-z0-9+.-]*:/i.test(raw) && parsed.origin === window.location.origin;
        if (isSameOriginRelative) return `${parsed.pathname}${parsed.search}${parsed.hash}`;
        if (allowHTTP && (parsed.protocol === 'http:' || parsed.protocol === 'https:')) return parsed.href;
    } catch {
        return '';
    }
    return '';
}

function publicBaseURL() {
    const raw = String(summaryState?.public_base_url || '').trim();
    if (raw) {
        try {
            const parsed = new URL(raw, window.location.origin);
            if (parsed.protocol === 'http:' || parsed.protocol === 'https:') {
                return parsed.toString().replace(/\/+$/, '');
            }
        } catch {
            // Use the current origin below.
        }
    }
    return window.location.origin;
}

function formatDateTime(value) {
    if (!value) return 'not set';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString([], {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
    });
}

function formatUptime(seconds) {
    const s = Math.round(seconds || 0);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

function applyNavState() {
    document.querySelectorAll('[data-public-link]').forEach((link) => {
        link.classList.toggle('active', link.dataset.publicLink === currentPage);
    });
}

function applyPageLayout() {
    if (publicSummarySection) {
        publicSummarySection.hidden = currentPage !== 'home';
    }
    if (publicFeedsSection) {
        publicFeedsSection.hidden = currentPage !== 'feeds' || isListenPage;
    }
    if (publicListenSection) {
        publicListenSection.hidden = !isListenPage;
    }
    if (publicAlertsSection) {
        publicAlertsSection.hidden = currentPage !== 'alerts';
    }
}

function setTextIfChanged(element, text) {
    if (element && element.textContent !== text) {
        element.textContent = text;
    }
}

function setNotice(text) {
    if (feedNotice) {
        feedNotice.hidden = !String(text || '').trim();
    }
    if (lastNoticeText === text) return;
    lastNoticeText = text;
    setTextIfChanged(feedNotice, text);
}

function summarySignature(summary) {
    return JSON.stringify({
        name: summary.name || '',
        hostname: summary.hostname || '',
        ip_address: summary.ip_address || '',
        operator: summary.operator || '',
        version: summary.version || '',
        git_commit: summary.git_commit || '',
        os: summary.os || '',
        architecture: summary.architecture || '',
        feed_count: summary.feed_count || 0,
        enabled_feed_count: summary.enabled_feed_count || 0,
        feeds_access: summary.feeds_access || '',
        admin_url: summary.admin_url || '',
        webrtc_enabled: Boolean(summary.webrtc_enabled),
        media_available: Boolean(summary.media_available),
        alerts_archive: summary.alerts_archive || '',
        tls: {
            enabled: Boolean(summary.tls?.enabled),
            https: Boolean(summary.tls?.https),
            host: summary.tls?.host || '',
            needs_setup: Boolean(summary.tls?.needs_setup),
            domains: summary.tls?.configured_domains || [],
        },
        uptime_bucket: currentPage === 'home' ? Math.floor((summary.uptime_seconds || 0) / 10) : 0,
    });
}

function feedsSignature(feeds) {
    return JSON.stringify({
        media_available: Boolean(summaryState?.media_available),
        webrtc_enabled: Boolean(summaryState?.webrtc_enabled),
        feeds: (feeds || []).map((feed) => {
            const tx = feed.transmitter || {};
            return {
                id: feed.id,
                enabled: Boolean(feed.enabled),
                site_names: tx.site_names || [tx.site_name || feed.name || ''],
                webrtc_enabled: Boolean(feed.webrtc_enabled),
                http_stream_enabled: Boolean(feed.http_stream_enabled),
            };
        }),
    });
}

function findFeedElement(name, feedId) {
    return Array.from(document.querySelectorAll(`[data-${name}]`))
        .find((element) => element.dataset[toDatasetKey(name)] === feedId) || null;
}

function toDatasetKey(name) {
    return name.replace(/-([a-z])/g, (_, char) => char.toUpperCase());
}

function publicWebRTCAudioElement(feedId) {
    feedId = String(feedId || '');
    let audio = findFeedElement('feed-audio', feedId) || Array.from(document.querySelectorAll('[data-feed-audio-sink]'))
        .find((element) => element.dataset.feedAudioSink === feedId) || null;
    if (!audio) {
        audio = document.createElement('audio');
        audio.dataset.feedAudioSink = feedId;
        audio.autoplay = true;
        audio.playsInline = true;
        audio.muted = false;
        audio.className = 'public-audio-sink';
        document.body.appendChild(audio);
    }
    audio.dataset.feedAudioSink = feedId;
    if (audio.dataset.hazeSinkBound !== '1') {
        audio.dataset.hazeSinkBound = '1';
        audio.addEventListener('pause', () => {
            const feedId = audio.dataset.feedAudioSink;
            const player = feedPlayers.get(feedId);
            recordWebRTCEvent(audio.dataset.feedAudioSink, 'audio_pause', {
                ready_state: audio.readyState,
                network_state: audio.networkState,
                ended: audio.ended,
                packets_recent: hasRecentWebRTCPackets(player),
            });
            if (player?.mode === 'webrtc' && audio.dataset.hazeUserPaused !== '1' && hasRecentWebRTCPackets(player)) {
                ensureWebRTCAudioPlaying(feedId, player, audio);
            }
        });
        audio.addEventListener('playing', () => {
            recordWebRTCEvent(audio.dataset.feedAudioSink, 'audio_playing', {
                ready_state: audio.readyState,
                network_state: audio.networkState,
            });
        });
    }
    return audio;
}

function renderSummary(summary) {
    summaryState = summary;
    const signature = summarySignature(summary);
    if (signature === lastSummarySignature) {
        return;
    }
    lastSummarySignature = signature;

    if (alertsLink) {
        alertsLink.hidden = summary.alerts_archive !== 'public' && !summary.capabilities?.public_alerts;
    }

    if (summary.admin_url) {
        adminLink.href = safePublicLink(summary.admin_url, { allowRelative: true }) || '/admin';
        setTextIfChanged(adminHint, summary.feeds_access === 'auth_required'
            ? 'Protected feeds need an authenticated admin session in this browser.'
            : 'Open the admin panel for protected feeds and station controls.');
    }

    const siteName = summary.on_air_name || summary.name || 'Haze Weather Radio';
    setTextIfChanged(publicSiteTitle, siteName);
    document.title = siteName;
    const description = document.querySelector('meta[name="description"]');
    if (description) {
        description.setAttribute('content', `Live weather radio feeds, alerts, and public status for ${siteName}.`);
    }

    const cards = [
        ['Operator', summary.operator || 'unconfigured'],
        ['IP Address', summary.ip_address || 'unknown'],
        ['Hostname', summary.hostname || 'unknown'],
        ['Feeds', `${summary.enabled_feed_count}/${summary.feed_count}`],
        ['Uptime', formatUptime(summary.uptime_seconds)],
    ];

    if (summaryCards) {
        summaryCards.innerHTML = cards.map(([label, value]) => `
            <div class="public-status-item">
                <span>${escapeHtml(label)}:</span>
                <strong>${escapeHtml(value)}</strong>
            </div>
        `).join('');
    }
    if (buildInfo) {
        buildInfo.innerHTML = [
            ['Version', summary.version || 'dev'],
            ['Commit', summary.git_commit || 'unknown'],
            ['System', [summary.os || 'unknown', summary.architecture || 'unknown'].join(' / ')],
        ].map(([label, value]) => `
            <span><b>${escapeHtml(label)}:</b><em>${escapeHtml(value)}</em></span>
        `).join('');
    }
    renderTLSNotice(summary.tls || {});
}

function renderTLSNotice(tls) {
    if (!publicTlsNotice || !publicTlsNoticeText) return;
    const show = Boolean(tls.actual_domain || tls.enabled || tls.needs_setup);
    publicTlsNotice.hidden = !show;
    if (!show) return;
    publicTlsNotice.dataset.state = tls.https ? 'ok' : (tls.needs_setup ? 'warn' : 'info');
    publicTlsNoticeText.textContent = tls.message || 'TLS status unavailable.';
}

function cardMarkup(feed) {
    const tx = feed.transmitter || {};
    const siteNames = (tx.site_names || [tx.site_name]).filter(Boolean).join(', ') || feed.name || 'Unnamed site';
    const feedId = String(feed.id || '');
    const nowPlaying = publicRuntimeTitle(feed.runtime);
    const remaining = publicRuntimeRemainingText(feed.runtime);
    const listeners = feed.listeners || {};
    const canWebRTC = feedCanWebRTC(feed);
    const canHTTP = feedCanHTTP(feed);
    const canPlay = canWebRTC || canHTTP;
    const prefs = normalizedFeedPreferences(feedId, feed);
    const feedState = !feed.enabled
        ? 'disabled'
        : (canPlay ? 'ready' : (summaryState?.media_available ? 'unavailable' : 'waiting'));
    const httpURL = canHTTP ? httpStreamURL(feedId, false, prefs.mode === 'http' ? prefs.codec : DEFAULT_HTTP_CODEC) : '';
    const httpSelected = prefs.mode === 'http';
    const playerControls = canPlay ? `
            <div class="public-feed-controls">
                <div class="public-feed-listeners" data-feed-listeners="${escapeHtml(feedId)}">
                    <span class="public-feed-listener-metric" data-feed-listener-current-metric="${escapeHtml(feedId)}" title="Concurrent listeners" aria-label="Concurrent listeners: ${escapeHtml(listenerCountLabel(listeners.current))}">
                        <i data-lucide="headphones" width="16" height="16"></i>
                        <strong data-feed-listener-current="${escapeHtml(feedId)}">${escapeHtml(listenerCountLabel(listeners.current))}</strong>
                    </span>
                    <span class="public-feed-listener-metric public-feed-listener-peak" data-feed-listener-peak-metric="${escapeHtml(feedId)}" title="${escapeHtml(peakListenerTitle(listeners))}" aria-label="Peak concurrent listeners: ${escapeHtml(peakListenerLabel(listeners.peak))}">
                        <i data-lucide="chart-no-axes-combined" width="16" height="16"></i>
                        <strong data-feed-listener-peak="${escapeHtml(feedId)}">${escapeHtml(peakListenerLabel(listeners.peak))}</strong>
                    </span>
                </div>
                <div class="public-feed-audio-row">
                    <audio class="public-feed-audio" data-feed-audio="${escapeHtml(feedId)}" data-feed-http-enabled="${canHTTP ? '1' : '0'}" ${httpSelected && canHTTP ? `src="${escapeHtml(httpURL)}"` : ''} controls playsinline preload="none"></audio>
                    ${canHTTP ? `
                        <button class="btn-action public-player-btn public-feed-copy-btn" type="button" data-feed-share="${escapeHtml(feedId)}" title="Copy HTTP stream link">
                            <i data-lucide="copy" width="15" height="15"></i>
                            <span>Copy</span>
                        </button>
                    ` : ''}
                </div>
            </div>
    ` : '';

    return `
        <article class="feed-card public-feed-card" data-feed-card="${escapeHtml(feedId)}" data-feed-state="${escapeHtml(feedState)}" data-feed-mode-selected="${escapeHtml(prefs.mode)}" role="link" tabindex="0" aria-label="Open ${escapeHtml(siteNames)} feed">
            <div class="public-feed-titlebar">
                <p class="feed-id">${escapeHtml(feedId)}</p>
                <h3 data-feed-title="${escapeHtml(feedId)}">${escapeHtml(siteNames)}</h3>
            </div>
            <div class="public-feed-main">
                <div class="public-feed-overview">
                    <div>
                        <p class="public-feed-now" data-feed-now="${escapeHtml(feedId)}">
                            <span class="public-feed-now-title" data-feed-now-title="${escapeHtml(feedId)}">${escapeHtml(nowPlaying)}</span>
                            <span class="public-feed-remaining" data-feed-remaining="${escapeHtml(feedId)}" ${remaining ? '' : 'hidden'}>${escapeHtml(remaining)}</span>
                        </p>
                    </div>
                </div>
            </div>
            ${playerControls}
        </article>
    `;
}

function createFeedCard(feed) {
    const template = document.createElement('template');
    template.innerHTML = cardMarkup(feed).trim();
    return template.content.firstElementChild;
}

function updateExistingFeedCard(card, feed) {
    const tx = feed.transmitter || {};
    const feedId = String(feed.id || '');
    const siteNames = (tx.site_names || [tx.site_name]).filter(Boolean).join(', ') || feed.name || 'Unnamed site';
    const canWebRTC = feedCanWebRTC(feed);
    const canHTTP = feedCanHTTP(feed);
    const canPlay = canWebRTC || canHTTP;
    const feedState = !feed.enabled
        ? 'disabled'
        : (canPlay ? 'ready' : (summaryState?.media_available ? 'unavailable' : 'waiting'));
    card.dataset.feedState = feedState;
    card.setAttribute('aria-label', `Open ${siteNames} feed`);
    setTextIfChanged(findFeedElement('feed-title', feedId), siteNames);
    setPublicFeedRuntime(feedId, feed.runtime);
    setPublicFeedListeners(feedId, feed.listeners);
    const mode = findFeedElement('feed-mode', feedId);
    if (mode) {
        mode.disabled = !canPlay;
        for (const option of mode.options) {
            option.disabled = option.value === 'webrtc' ? !canWebRTC : !canHTTP;
        }
    }
    const codec = findFeedElement('feed-codec', feedId);
    if (codec) codec.disabled = !canPlay;
    const audio = findFeedElement('feed-audio', feedId);
    if (audio) {
        audio.dataset.feedHttpEnabled = canHTTP ? '1' : '0';
        if (!canHTTP) {
            pauseFeedHTTPAudio(feedId, { clear: true });
        }
    }
    updateFeedModePresentation(feedId, { resetStatus: false });
    const play = findFeedElement('feed-play', feedId);
    if (play) {
        play.dataset.feedPlayable = canPlay ? '1' : '0';
        play.disabled = feedPlayers.has(feedId) || !canPlay;
    }
    const stop = findFeedElement('feed-stop', feedId);
    if (stop) stop.disabled = !feedPlayers.has(feedId);
    const share = findFeedElement('feed-share', feedId);
    if (share) share.disabled = !canHTTP;
    if (!feedPlayers.has(feedId)) {
        setPlayerStatus(feedId, canPlay
            ? (canWebRTC ? 'Ready' : (summaryState?.media_available ? 'Ready' : 'Waiting for playout audio'))
            : (feed.enabled ? 'Streaming unavailable' : 'Feed disabled'));
    }
}

function feedCanWebRTC(feed) {
    return Boolean(feed.enabled && feed.webrtc_enabled && summaryState?.webrtc_enabled);
}

function feedCanHTTP(feed) {
    return Boolean(feed.enabled && feed.http_stream_enabled && summaryState?.media_available);
}

function normalizedFeedPreferences(feedId, feed = null) {
    const prefs = normalizePublicFeedGlobalPreferences(feed);
    feedPreferences.set(feedId, prefs);
    return prefs;
}

function codecOptionsForMode(mode) {
    return mode === 'http' ? HTTP_CODECS : WEBRTC_CODECS;
}

function selectedFeedMode(feedId) {
    return normalizedFeedPreferences(feedId).mode;
}

function selectedFeedCodec(feedId) {
    return normalizedFeedPreferences(feedId).codec;
}

function setFeedMode(feedId, mode) {
    setPublicFeedMode(mode);
}

function defaultCodecForMode(mode) {
    return mode === 'http' ? DEFAULT_HTTP_CODEC : DEFAULT_WEBRTC_CODEC;
}

function updateFeedCodecSelect(feedId) {
    renderPublicFeedToolbar(currentPublicFeeds);
}

function updateFeedModePresentation(feedId, { resetStatus = true } = {}) {
    feedId = String(feedId || '');
    const prefs = normalizedFeedPreferences(feedId);
    const card = findFeedElement('feed-card', feedId);
    if (card) {
        card.dataset.feedModeSelected = prefs.mode;
    }
    const isHTTP = prefs.mode === 'http';
    const audio = findFeedElement('feed-audio', feedId);
    const hasHTTP = audio?.dataset.feedHttpEnabled === '1';
    if (audio) {
        audio.controls = true;
        audio.hidden = isHTTP ? !hasHTTP : !summaryState?.webrtc_enabled;
        if (isHTTP && hasHTTP) {
            updateFeedHTTPAudio(feedId);
        } else {
            audio.dataset.hazeHttpWanted = '0';
            clearHTTPAudioReconnect(feedId, true);
            if (audio.hasAttribute('src')) {
                audio.pause();
                audio.removeAttribute('src');
                audio.load();
            }
        }
    }
    if (resetStatus) {
        setPlayerStatus(feedId, isHTTP ? 'Use the audio controls to listen' : 'WebRTC selected');
    }
}

function updateFeedHTTPAudio(feedId) {
    feedId = String(feedId || '');
    const audio = findFeedElement('feed-audio', feedId);
    if (!audio || audio.dataset.feedHttpEnabled !== '1') return;
    const prefs = normalizedFeedPreferences(feedId);
    const nextURL = httpStreamURL(feedId, false, prefs.codec);
    if (audio.getAttribute('src') === nextURL) return;
    const wasPlaying = !audio.paused && !audio.ended;
    audio.srcObject = null;
    audio.src = nextURL;
    audio.load();
    if (wasPlaying) {
        audio.play().catch(() => {
            setPlayerStatus(feedId, 'Press Play in the audio control');
        });
    }
}

function pauseFeedHTTPAudio(feedId, { clear = false } = {}) {
    const audio = findFeedElement('feed-audio', feedId);
    if (!audio) return;
    audio.dataset.hazeHttpWanted = '0';
    clearHTTPAudioReconnect(feedId, true);
    audio.pause();
    if (clear) {
        audio.removeAttribute('src');
        audio.load();
    }
}

function clearHTTPAudioReconnect(feedId, resetAttempts = false) {
    feedId = String(feedId || '');
    const timer = httpReconnectTimers.get(feedId);
    if (timer) {
        window.clearTimeout(timer);
        httpReconnectTimers.delete(feedId);
    }
    if (resetAttempts) {
        httpReconnectAttempts.delete(feedId);
    }
}

function scheduleHTTPAudioReconnect(feedId, audio, { stalled = false } = {}) {
    feedId = String(feedId || '');
    if (!feedId
        || !audio
        || audio.dataset.hazeHttpWanted !== '1'
        || selectedFeedMode(feedId) !== 'http'
        || httpReconnectTimers.has(feedId)) {
        return;
    }
    const attempt = Math.max(0, Number(httpReconnectAttempts.get(feedId) || 0));
    const delay = stalled
        ? HTTP_STALL_RECONNECT_MS
        : Math.min(HTTP_RECONNECT_MAX_MS, HTTP_RECONNECT_BASE_MS * (2 ** attempt));
    const timer = window.setTimeout(async () => {
        httpReconnectTimers.delete(feedId);
        if (audio.dataset.hazeHttpWanted !== '1' || selectedFeedMode(feedId) !== 'http') {
            return;
        }
        httpReconnectAttempts.set(feedId, attempt + 1);
        audio.dataset.hazeHttpRestarting = '1';
        setPlayerStatus(feedId, 'Reconnecting HTTP audio');
        try {
            audio.load();
            await audio.play();
        } catch {
            audio.dataset.hazeHttpRestarting = '0';
            scheduleHTTPAudioReconnect(feedId, audio);
        }
    }, delay);
    httpReconnectTimers.set(feedId, timer);
}

function httpStreamURL(feedId, absolute = false, codec = null) {
    const url = new URL(`${API_BASE}/feed/audio`, absolute ? publicBaseURL() : window.location.origin);
    url.searchParams.set('feed', feedId);
    url.searchParams.set('codec', normalizeHTTPCodec(codec || selectedFeedCodec(feedId)));
    const token = publicAudioToken();
    if (token) url.searchParams.set('token', token);
    return absolute ? url.toString() : `${url.pathname}${url.search}`;
}

function publicAudioToken() {
    if (summaryState?.feeds_access !== 'auth_required') {
        return '';
    }
    return session.token || '';
}

function listenPageURL(feedId, absolute = false, codec = null, options = {}) {
    const url = new URL('/listen', absolute ? publicBaseURL() : window.location.origin);
    url.searchParams.set('feed', feedId);
    url.searchParams.set('codec', normalizeHTTPCodec(codec || selectedFeedCodec(feedId)));
    if (options.includeToken) {
        const token = publicAudioToken();
        if (token) url.searchParams.set('token', token);
    }
    return absolute ? url.toString() : `${url.pathname}${url.search}`;
}

function normalizeHTTPCodec(codec) {
    const value = String(codec || '').trim().toLowerCase().replaceAll('-', '_');
    return HTTP_CODEC_VALUES.has(value) ? value : DEFAULT_HTTP_CODEC;
}

function publicFeedToolbarElement() {
    if (!publicFeedsSection || !feedsGrid) return null;
    let toolbar = document.getElementById('publicFeedToolbar');
    if (!toolbar) {
        toolbar = document.createElement('div');
        toolbar.id = 'publicFeedToolbar';
        toolbar.className = 'public-feed-toolbar';
        publicFeedsSection.insertBefore(toolbar, feedsGrid);
    }
    return toolbar;
}

function renderPublicFeedToolbar(feeds) {
    const toolbar = publicFeedToolbarElement();
    if (!toolbar) return;
    const canHTTP = (feeds || []).some(feedCanHTTP);
    const canWebRTC = (feeds || []).some(feedCanWebRTC);
    const prefs = normalizePublicFeedGlobalPreferences();
    toolbar.hidden = !canHTTP && !canWebRTC;
    if (toolbar.hidden) return;
    const modeOptions = [
        ['http', 'HTTP', canHTTP],
        ['webrtc', 'WebRTC', canWebRTC],
    ].map(([value, label, enabled]) => `
        <option value="${value}" ${prefs.mode === value ? 'selected' : ''} ${enabled ? '' : 'disabled'}>${label}</option>
    `).join('');
    const codecOptions = codecOptionsForMode(prefs.mode).map(([value, label]) => `
        <option value="${value}" ${prefs.codec === value ? 'selected' : ''}>${label}</option>
    `).join('');
    toolbar.innerHTML = `
        <label class="public-listen-field public-feed-global-field">
            <span>Format</span>
            <select data-feed-global-mode>
                ${modeOptions}
            </select>
        </label>
        <label class="public-listen-field public-feed-global-field">
            <span>Codec</span>
            <select data-feed-global-codec>
                ${codecOptions}
            </select>
        </label>
    `;
    attachPublicFeedToolbar();
}

function attachPublicFeedToolbar() {
    const toolbar = document.getElementById('publicFeedToolbar');
    if (!toolbar) return;
    toolbar.querySelector('[data-feed-global-mode]')?.addEventListener('change', (event) => {
        setPublicFeedMode(event.target.value);
    });
    toolbar.querySelector('[data-feed-global-codec]')?.addEventListener('change', (event) => {
        setPublicFeedCodec(event.target.value);
    });
}

function setPublicFeedMode(mode) {
    const nextMode = mode === 'webrtc' ? 'webrtc' : 'http';
    if (publicFeedGlobalPreferences.mode !== nextMode) {
        publicFeedGlobalPreferences.mode = nextMode;
        publicFeedGlobalPreferences.codec = defaultCodecForMode(nextMode);
    }
    writePublicFeedGlobalPreferences();
    renderPublicFeedToolbar(currentPublicFeeds);
    applyPublicFeedPreferencesToCards({ stopWebRTC: true });
}

function setPublicFeedCodec(codec) {
    const prefs = normalizePublicFeedGlobalPreferences();
    const allowed = codecOptionsForMode(prefs.mode).map(([value]) => value);
    publicFeedGlobalPreferences.codec = allowed.includes(codec) ? codec : defaultCodecForMode(prefs.mode);
    writePublicFeedGlobalPreferences();
    renderPublicFeedToolbar(currentPublicFeeds);
    applyPublicFeedPreferencesToCards({ stopWebRTC: prefs.mode === 'webrtc' });
}

function applyPublicFeedPreferencesToCards({ stopWebRTC = false } = {}) {
    for (const feed of currentPublicFeeds) {
        const feedId = String(feed.id || '');
        if (!feedId) continue;
        feedPreferences.set(feedId, normalizePublicFeedGlobalPreferences(feed));
        feedCodecFallbacks.delete(feedId);
        feedReconnectBackoffs.delete(feedId);
        if (stopWebRTC) {
            stopFeed(feedId, { silent: true });
        }
        updateFeedModePresentation(feedId, { resetStatus: true });
    }
    window.lucide?.createIcons();
}

function updateFeedRuntime(feeds) {
    feeds.forEach((feed) => {
        const feedId = String(feed.id || '');
        setPublicFeedRuntime(feedId, feed.runtime);
        setPublicFeedListeners(feedId, feed.listeners);
    });
}

function renderFeeds(feeds) {
    currentPublicFeeds = feeds || [];
    if (summaryState?.feeds_access === 'disabled') {
        setNotice('Public feeds are disabled on this system.');
        const toolbar = publicFeedToolbarElement();
        if (toolbar) toolbar.hidden = true;
        if (lastFeedsSignature !== 'disabled') {
            lastFeedsSignature = 'disabled';
            feedsGrid.innerHTML = '';
        }
        return;
    }

    if (!feeds.length) {
        setNotice('No feeds are currently configured.');
        const toolbar = publicFeedToolbarElement();
        if (toolbar) toolbar.hidden = true;
        if (lastFeedsSignature !== 'empty') {
            lastFeedsSignature = 'empty';
            feedsGrid.innerHTML = '<article class="feed-card empty">No feeds configured.</article>';
        }
        return;
    }

    if (summaryState?.feeds_access === 'auth_required') {
        setNotice('Feed details require an authenticated admin session.');
    } else {
        setNotice('');
    }
    renderPublicFeedToolbar(feeds);
    const signature = feedsSignature(feeds);
    if (signature === lastFeedsSignature) {
        updateFeedRuntime(feeds);
        return;
    }
    stopRemovedPlayers(new Set(feeds.map((feed) => String(feed.id || ''))));
    lastFeedsSignature = signature;
    const fragment = document.createDocumentFragment();
    for (const feed of feeds) {
        const feedId = String(feed.id || '');
        const existing = findFeedElement('feed-card', feedId);
        const canPlay = feedCanWebRTC(feed) || feedCanHTTP(feed);
        if (existing && feedPlayers.has(feedId) && canPlay) {
            updateExistingFeedCard(existing, feed);
            fragment.appendChild(existing);
        } else {
            if (!canPlay && feedPlayers.has(feedId)) {
                stopFeed(feedId, { silent: true });
            }
            fragment.appendChild(createFeedCard(feed));
        }
    }
    feedsGrid.replaceChildren(fragment);
    updateFeedRuntime(feeds);
    attachFeedControls();
    reattachActivePlayers();
    window.lucide?.createIcons();
}

function attachFeedControls() {
    feedsGrid.querySelectorAll('[data-feed-card]').forEach((card) => {
        if (card.dataset.hazeBound === '1') return;
        card.dataset.hazeBound = '1';
        const open = () => {
            const feedId = card.dataset.feedCard;
            if (!feedId) return;
            window.location.assign(listenPageURL(feedId, false, null, { includeToken: true }));
        };
        card.addEventListener('click', (event) => {
            if (isFeedCardControlClick(event)) return;
            open();
        });
        card.addEventListener('keydown', (event) => {
            if (event.key !== 'Enter' && event.key !== ' ') return;
            if (isFeedCardControlClick(event)) return;
            event.preventDefault();
            open();
        });
    });
    feedsGrid.querySelectorAll('[data-feed-mode]').forEach((select) => {
        if (select.dataset.hazeBound === '1') return;
        select.dataset.hazeBound = '1';
        select.addEventListener('change', () => {
            const feedId = select.dataset.feedMode;
            feedCodecFallbacks.delete(feedId);
            feedReconnectBackoffs.delete(feedId);
            setFeedMode(feedId, select.value);
            stopFeed(feedId, { silent: true });
            setPlayerStatus(feedId, select.value === 'http' ? 'Use the audio controls to listen' : 'WebRTC selected');
        });
    });
    feedsGrid.querySelectorAll('[data-feed-codec]').forEach((select) => {
        if (select.dataset.hazeBound === '1') return;
        select.dataset.hazeBound = '1';
        select.addEventListener('change', () => {
            const feedId = select.dataset.feedCodec;
            const prefs = normalizedFeedPreferences(feedId);
            prefs.codec = select.value;
            feedPreferences.set(feedId, prefs);
            feedCodecFallbacks.delete(feedId);
            feedReconnectBackoffs.delete(feedId);
            if (prefs.mode === 'http') {
                updateFeedHTTPAudio(feedId);
                setPlayerStatus(feedId, 'Format changed');
            } else {
                stopFeed(feedId, { silent: true });
                setPlayerStatus(feedId, 'Codec changed');
            }
        });
    });
    feedsGrid.querySelectorAll('[data-feed-play]').forEach((button) => {
        if (button.dataset.hazeBound === '1') return;
        button.dataset.hazeBound = '1';
        button.addEventListener('click', () => startFeed(button.dataset.feedPlay));
    });
    feedsGrid.querySelectorAll('[data-feed-stop]').forEach((button) => {
        if (button.dataset.hazeBound === '1') return;
        button.dataset.hazeBound = '1';
        button.addEventListener('click', () => stopFeed(button.dataset.feedStop));
    });
    feedsGrid.querySelectorAll('[data-feed-share]').forEach((button) => {
        if (button.dataset.hazeBound === '1') return;
        button.dataset.hazeBound = '1';
        button.addEventListener('click', () => copyHTTPLink(button.dataset.feedShare));
    });
    feedsGrid.querySelectorAll('[data-feed-volume]').forEach((input) => {
        if (input.dataset.hazeBound === '1') return;
        input.dataset.hazeBound = '1';
        input.addEventListener('input', () => {
            const player = feedPlayers.get(input.dataset.feedVolume);
            if (player?.audio) {
                player.audio.volume = Number(input.value);
            }
        });
    });
    feedsGrid.querySelectorAll('[data-feed-audio]').forEach((audio) => {
        if (audio.dataset.hazeBound === '1') return;
        audio.dataset.hazeBound = '1';
        const startNativeWebRTC = () => {
            const feedId = audio.dataset.feedAudio;
            if (normalizedFeedPreferences(feedId).mode !== 'webrtc') return;
            if (feedPlayers.has(feedId) || audio.dataset.hazeWebRTCStarting === '1') return;
            audio.dataset.hazeWebRTCStarting = '1';
            audio.dataset.hazeUserPaused = '0';
            startFeedWebRTC(feedId).finally(() => {
                audio.dataset.hazeWebRTCStarting = '0';
            });
        };
        audio.addEventListener('click', startNativeWebRTC);
        audio.addEventListener('play', () => {
            const feedId = audio.dataset.feedAudio;
            if (normalizedFeedPreferences(feedId).mode === 'webrtc') {
                audio.dataset.hazeUserPaused = '0';
                const player = feedPlayers.get(feedId);
                if (player?.trackAttached) {
                    resumeFeedAudio(feedId, player);
                } else {
                    startNativeWebRTC();
                }
                return;
            }
            audio.dataset.hazeHttpWanted = '1';
            setPlayerStatus(feedId, 'Starting HTTP audio');
        });
        audio.addEventListener('playing', () => {
            if (normalizedFeedPreferences(audio.dataset.feedAudio).mode !== 'http') return;
            audio.dataset.hazeHttpWanted = '1';
            audio.dataset.hazeHttpRestarting = '0';
            clearHTTPAudioReconnect(audio.dataset.feedAudio, true);
            setPlayerStatus(audio.dataset.feedAudio, 'Playing over HTTP');
        });
        audio.addEventListener('waiting', () => {
            if (normalizedFeedPreferences(audio.dataset.feedAudio).mode !== 'http') return;
            setPlayerStatus(audio.dataset.feedAudio, 'Browser buffering audio');
            scheduleHTTPAudioReconnect(audio.dataset.feedAudio, audio, { stalled: true });
        });
        audio.addEventListener('stalled', () => {
            if (normalizedFeedPreferences(audio.dataset.feedAudio).mode !== 'http') return;
            setPlayerStatus(audio.dataset.feedAudio, 'HTTP audio stalled');
            scheduleHTTPAudioReconnect(audio.dataset.feedAudio, audio, { stalled: true });
        });
        audio.addEventListener('pause', () => {
            const feedId = audio.dataset.feedAudio;
            if (normalizedFeedPreferences(feedId).mode === 'webrtc') {
                const player = feedPlayers.get(feedId);
                if (player && !player.stopping) {
                    audio.dataset.hazeUserPaused = '1';
                    audio.dataset.hazePlayerState = 'paused';
                    setPlayerStatus(feedId, 'Paused');
                }
                return;
            }
            if (audio.dataset.hazeHttpRestarting !== '1' && !audio.ended && !audio.error) {
                audio.dataset.hazeHttpWanted = '0';
                clearHTTPAudioReconnect(audio.dataset.feedAudio, true);
            }
            setPlayerStatus(audio.dataset.feedAudio, 'Paused');
        });
        audio.addEventListener('ended', () => {
            if (normalizedFeedPreferences(audio.dataset.feedAudio).mode !== 'http') return;
            setPlayerStatus(audio.dataset.feedAudio, 'Reconnecting HTTP audio');
            scheduleHTTPAudioReconnect(audio.dataset.feedAudio, audio);
        });
        audio.addEventListener('error', () => {
            if (normalizedFeedPreferences(audio.dataset.feedAudio).mode !== 'http') return;
            setPlayerStatus(audio.dataset.feedAudio, 'HTTP audio error');
            scheduleHTTPAudioReconnect(audio.dataset.feedAudio, audio);
        });
    });
}

function isFeedCardControlClick(event) {
    const target = event?.target;
    return Boolean(target?.closest?.('button, a, input, select, audio, label, [role="button"]'));
}

function setPlayerStatus(feedId, message) {
    const status = findFeedElement('feed-status', feedId);
    if (status) {
        setTextIfChanged(status, message);
    }
}

function setPlayerButtons(feedId, playing) {
    const playButton = findFeedElement('feed-play', feedId);
    const stopButton = findFeedElement('feed-stop', feedId);
    if (playButton) playButton.disabled = playing || playButton.dataset.feedPlayable !== '1';
    if (stopButton) stopButton.disabled = !playing;
}

function clearPlayerTimer(player, key) {
    const timer = player?.[key];
    if (timer) {
        window.clearTimeout(timer);
        player[key] = null;
    }
}

function clearPlayerInterval(player, key) {
    const timer = player?.[key];
    if (timer) {
        window.clearInterval(timer);
        player[key] = null;
    }
}

function setHealthyWebRTCStatus(feedId, player, audio = player?.audio) {
    cancelWebRTCReconnect(player);
    clearPlayerTimer(player, 'connectionStateTimer');
    clearPlayerTimer(player, 'disconnectReconnectTimer');
    clearPlayerTimer(player, 'mediaEventTimer');
    if (!isActivePlayer(feedId, player)) return;
    if (audio?.dataset.hazePlayerState === 'play-blocked' || audio?.dataset.hazePlayerState === 'needs-play') {
        setPlayerStatus(feedId, 'Press Play to start audio');
        return;
    }
    ensureWebRTCAudioPlaying(feedId, player, audio);
    setPlayerStatus(feedId, audio?.paused ? 'Audio ready' : 'Playing');
}

function ensureWebRTCAudioPlaying(feedId, player, audio = player?.audio) {
    if (!isActivePlayer(feedId, player) || player?.mode !== 'webrtc' || !audio) return;
    if (audio.dataset.hazeUserPaused === '1') return;
    keepWebRTCAudioLive(feedId, player, audio);
    if (!audio.paused || audio.dataset.hazePlayerState === 'play-blocked' || audio.dataset.hazePlayerState === 'needs-play') return;
    recordWebRTCEvent(feedId, 'audio_resume_attempt', {
        ready_state: audio.readyState,
        network_state: audio.networkState,
        has_live_track: hasLiveWebRTCAudioTrack(player),
        packets_recent: hasRecentWebRTCPackets(player),
    });
    const playPromise = audio.play();
    if (playPromise && typeof playPromise.catch === 'function') {
        playPromise
            .then(() => {
                if (isActivePlayer(feedId, player)) {
                    audio.dataset.hazePlayerState = audio.paused ? 'audio-ready' : 'playing';
                    recordWebRTCEvent(feedId, 'audio_resume_ok', {
                        paused: audio.paused,
                        ready_state: audio.readyState,
                    });
                }
            })
            .catch((error) => {
                if (isActivePlayer(feedId, player)) {
                    audio.dataset.hazePlayerState = 'needs-play';
                    setPlayerStatus(feedId, 'Press Play to start audio');
                    recordWebRTCEvent(feedId, 'audio_resume_blocked', {
                        error: error?.name || 'play_failed',
                    });
                }
            });
    }
}

function keepWebRTCAudioLive(feedId, player, audio = player?.audio) {
    if (!isActivePlayer(feedId, player) || player?.mode !== 'webrtc' || !audio) return;
    if (audio.dataset.hazeUserPaused === '1') return;
    let repaired = false;
    if (audio.muted) {
        audio.muted = false;
        repaired = true;
    }
    const tracks = [
        ...(audio.srcObject?.getAudioTracks?.() || []),
        ...(player?.remoteStream?.getAudioTracks?.() || []),
    ];
    tracks.forEach((track) => {
        if (track && track.enabled === false) {
            track.enabled = true;
            repaired = true;
        }
    });
    if (repaired) {
        recordWebRTCEvent(feedId, 'audio_liveness_repaired', {
            track_count: tracks.length,
            ready_state: audio.readyState,
            network_state: audio.networkState,
            packets_recent: hasRecentWebRTCPackets(player),
        });
    }
    if (player.audioOutputMixer?.context?.state === 'suspended') {
        player.audioOutputMixer.context.resume().catch(() => {});
    }
    recoverMutedWebRTCOutputMixer(feedId, player, audio, 'watchdog');
}

function recoverMutedWebRTCOutputMixer(feedId, player, audio = player?.audio, reason = 'muted_output_track') {
    const mixer = player?.audioOutputMixer;
    if (!mixer || !audio || mixer.outputStream !== audio.srcObject) return false;
    const outputTracks = mixer.outputStream?.getAudioTracks?.() || [];
    if (!outputTracks.some((track) => track?.muted || track?.readyState !== 'live')) return false;
    if (!hasRecentWebRTCPackets(player) && hasHardStaleWebRTCPackets(player)) return false;
    const now = Date.now();
    if (now - Number(player.lastOutputMixerRecoveryAt || 0) < WEBRTC_OUTPUT_MIXER_RECOVERY_COOLDOWN_MS) return false;
    const windowStartedAt = Number(player.outputMixerRecoveryWindowAt || 0);
    if (!windowStartedAt || now - windowStartedAt > WEBRTC_OUTPUT_MIXER_RECOVERY_WINDOW_MS) {
        player.outputMixerRecoveryWindowAt = now;
        player.outputMixerRecoveryCount = 0;
    }
    player.outputMixerRecoveryCount = Number(player.outputMixerRecoveryCount || 0) + 1;
    player.lastOutputMixerRecoveryAt = now;
    const sourceStream = mixer.sourceStream;
    if (player.outputMixerRecoveryCount >= WEBRTC_OUTPUT_MIXER_RECOVERY_LIMIT) {
        player.outputMixerBypassUntil = now + WEBRTC_OUTPUT_MIXER_BYPASS_MS;
        recordWebRTCEvent(feedId, 'audio_output_mixer_bypassed', {
            reason,
            recovery_count: player.outputMixerRecoveryCount,
            bypass_ms: WEBRTC_OUTPUT_MIXER_BYPASS_MS,
            packets_recent: hasRecentWebRTCPackets(player),
        });
        closeWebRTCAudioOutput(player);
        audio.srcObject = sourceStream;
        ensureWebRTCAudioPlaying(feedId, player, audio);
        return true;
    }
    recordWebRTCEvent(feedId, 'audio_output_mixer_rebind', {
        reason,
        track_count: outputTracks.length,
        context_state: mixer.context?.state || '',
        packets_recent: hasRecentWebRTCPackets(player),
    });
    closeWebRTCAudioOutput(player);
    bindWebRTCAudioOutput(feedId, player, sourceStream, audio);
    ensureWebRTCAudioPlaying(feedId, player, audio);
    return true;
}

function closeWebRTCAudioOutput(player) {
    const mixer = player?.audioOutputMixer;
    if (!mixer) return;
    player.audioOutputMixer = null;
    if (mixer.resumeFallbackTimer) {
        window.clearTimeout(mixer.resumeFallbackTimer);
    }
    if (mixer.retryTimer) {
        window.clearTimeout(mixer.retryTimer);
    }
    if (mixer.outputTrackMuteTimer) {
        window.clearTimeout(mixer.outputTrackMuteTimer);
    }
    if (mixer.context) {
        mixer.context.onstatechange = null;
    }
    for (const track of mixer.outputStream?.getAudioTracks?.() || []) {
        track.onmute = null;
        track.onunmute = null;
        track.onended = null;
    }
    try {
        mixer.oscillator?.stop();
    } catch {
        // The oscillator may already be stopped during reconnect cleanup.
    }
    try {
        mixer.source?.disconnect();
        mixer.gain?.disconnect();
        mixer.oscillator?.disconnect();
    } catch {
        // Best-effort Web Audio cleanup.
    }
    if (mixer.context?.state !== 'closed') {
        mixer.context?.close?.().catch(() => {});
    }
}

function bindWebRTCAudioOutput(feedId, player, sourceStream, audio) {
    if (!sourceStream || !audio) return sourceStream;
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (webRTCOutputMixerDisabled() || (player?.outputMixerBypassUntil && player.outputMixerBypassUntil > Date.now())) {
        closeWebRTCAudioOutput(player);
        audio.srcObject = sourceStream;
        return sourceStream;
    }
    if (!AudioContextCtor || !sourceStream.getAudioTracks?.().length) {
        closeWebRTCAudioOutput(player);
        audio.srcObject = sourceStream;
        return sourceStream;
    }
    if (player.audioOutputMixer?.sourceStream === sourceStream) {
        audio.srcObject = player.audioOutputMixer.outputStream;
        player.audioOutputMixer.context?.resume?.().catch(() => {});
        return player.audioOutputMixer.outputStream;
    }
    closeWebRTCAudioOutput(player);
    try {
        const context = new AudioContextCtor({ latencyHint: 'interactive' });
        const destination = context.createMediaStreamDestination();
        const source = context.createMediaStreamSource(sourceStream);
        const oscillator = context.createOscillator();
        const gain = context.createGain();
        oscillator.type = 'sine';
        oscillator.frequency.value = WEBRTC_OUTPUT_BED_FREQUENCY;
        gain.gain.value = WEBRTC_OUTPUT_BED_GAIN;
        source.connect(destination);
        oscillator.connect(gain);
        gain.connect(destination);
        oscillator.start();
        const outputStream = destination.stream;
        player.audioOutputMixer = { context, source, oscillator, gain, destination, sourceStream, outputStream };
        context.onstatechange = () => {
            if (player.audioOutputMixer?.context !== context) return;
            recordWebRTCEvent(feedId, 'audio_output_mixer_state', {
                context_state: context.state,
                packets_recent: hasRecentWebRTCPackets(player),
            });
            if (context.state === 'suspended') {
                context.resume?.().catch(() => {});
            }
        };
        for (const track of outputStream.getAudioTracks?.() || []) {
            track.onmute = () => {
                if (player.audioOutputMixer?.outputStream !== outputStream) return;
                if (player.audioOutputMixer.outputTrackMuteTimer) {
                    window.clearTimeout(player.audioOutputMixer.outputTrackMuteTimer);
                }
                player.audioOutputMixer.outputTrackMuteTimer = window.setTimeout(() => {
                    if (player.audioOutputMixer?.outputStream !== outputStream) {
                        return;
                    }
                    if (hasRecentWebRTCPackets(player) || !hasHardStaleWebRTCPackets(player)) {
                        recoverMutedWebRTCOutputMixer(feedId, player, audio, 'output_track_mute_event');
                        return;
                    }
                    player.audioOutputMixer.outputTrackMutedReported = true;
                    recordWebRTCEvent(feedId, 'audio_output_track_mute', {
                        track_id: track.id || '',
                        ready_state: track.readyState || '',
                        context_state: context.state,
                        packets_recent: hasRecentWebRTCPackets(player),
                        packet_source_age_ms: webRTCPacketSourceAgeMS(player),
                    });
                }, WEBRTC_TRACK_MUTE_GRACE_MS);
                context.resume?.().catch(() => {});
            };
            track.onunmute = () => {
                if (player.audioOutputMixer?.outputStream !== outputStream) return;
                markWebRTCPlaybackProgress(feedId, player, audio);
                if (!hasRecentWebRTCPackets(player)) {
                    ensureWebRTCAudioPlaying(feedId, player, audio);
                    return;
                }
                if (player.audioOutputMixer.outputTrackMuteTimer) {
                    window.clearTimeout(player.audioOutputMixer.outputTrackMuteTimer);
                    player.audioOutputMixer.outputTrackMuteTimer = null;
                }
                if (player.audioOutputMixer.outputTrackMutedReported) {
                    player.audioOutputMixer.outputTrackMutedReported = false;
                    recordWebRTCEvent(feedId, 'audio_output_track_unmute', {
                        track_id: track.id || '',
                        ready_state: track.readyState || '',
                        context_state: context.state,
                        packets_recent: hasRecentWebRTCPackets(player),
                    });
                }
            };
            track.onended = () => {
                if (player.audioOutputMixer?.outputStream !== outputStream) return;
                recordWebRTCEvent(feedId, 'audio_output_track_ended', {
                    track_id: track.id || '',
                    context_state: context.state,
                });
            };
        }
        audio.srcObject = outputStream;
        context.resume?.().catch(() => {});
        player.audioOutputMixer.resumeFallbackTimer = window.setTimeout(() => {
            if (player.audioOutputMixer?.outputStream !== outputStream || context.state === 'running') return;
            recordWebRTCEvent(feedId, 'audio_output_mixer_resume_retry', {
                context_state: context.state,
            });
            context.resume?.().catch(() => {});
        }, WEBRTC_OUTPUT_MIXER_RESUME_GRACE_MS);
        recordWebRTCEvent(feedId, 'audio_output_mixer_bound', {
            source_track_count: sourceStream.getAudioTracks?.().length || 0,
            output_track_count: outputStream.getAudioTracks?.().length || 0,
        });
        return outputStream;
    } catch (error) {
        closeWebRTCAudioOutput(player);
        audio.srcObject = sourceStream;
        recordWebRTCEvent(feedId, 'audio_output_mixer_failed', {
            error: error?.name || 'mixer_failed',
        });
        return sourceStream;
    }
}

function hasRecentWebRTCPackets(player, now = Date.now()) {
    const lastProgressAt = Number(player?.lastAudioProgressAt || player?.lastPacketAt || 0);
    return lastProgressAt > 0 && now - lastProgressAt <= WEBRTC_RECENT_PACKET_GRACE_MS;
}

function webRTCPacketSourceAgeMS(player, now = Date.now()) {
    const lastProgressAt = Number(player?.lastAudioProgressAt || player?.lastPacketAt || 0);
    const startedAt = Number(player?.startedAt || 0);
    const sourceAt = lastProgressAt || startedAt;
    return sourceAt > 0 ? now - sourceAt : Number.POSITIVE_INFINITY;
}

function hasHardStaleWebRTCPackets(player, now = Date.now()) {
    return webRTCPacketSourceAgeMS(player, now) >= WEBRTC_HARD_PACKET_STALE_MS;
}

function markWebRTCPlaybackProgress(feedId, player, audio = player?.audio) {
    if (!player || !audio || audio.paused || audio.ended) return false;
    const currentTime = Number(audio.currentTime || 0);
    const previousTime = Number(player.lastPlaybackTime || 0);
    player.lastPlaybackTime = currentTime;
    if (currentTime <= previousTime + 0.02) return false;
    player.lastPlaybackProgressAt = Date.now();
    markWebRTCPacketsRecent(feedId, player);
    return true;
}

function startWebRTCPlaybackWatchdog(feedId, player) {
    clearPlayerInterval(player, 'playbackWatchdogTimer');
    if (!player) return;
    player.playbackWatchdogTimer = window.setInterval(() => {
        if (!isActivePlayer(feedId, player) || player.mode !== 'webrtc' || player.stopping) {
            clearPlayerInterval(player, 'playbackWatchdogTimer');
            return;
        }
        markWebRTCPlaybackProgress(feedId, player, player.audio);
        if (hasRecentWebRTCPackets(player)) {
            keepWebRTCAudioLive(feedId, player, player.audio);
            ensureWebRTCAudioPlaying(feedId, player, player.audio);
        }
    }, WEBRTC_PLAYBACK_WATCHDOG_MS);
}

function webRTCStatsDelta(previous, snapshot, field, reset = false) {
    if (!previous || reset) return 0;
    return Number(snapshot?.[field] || 0) - Number(previous?.[field] || 0);
}

function hasLiveWebRTCAudioTrack(player) {
    const tracks = [
        ...(player?.audio?.srcObject?.getAudioTracks?.() || []),
        ...(player?.remoteStream?.getAudioTracks?.() || []),
    ];
    return tracks.some((track) => track.readyState === 'live');
}

function hasUsableWebRTCAudio(player) {
    return Boolean(player?.trackAttached || hasLiveWebRTCAudioTrack(player));
}

function shouldReconnectWebRTCForMissingPackets(player) {
    return !hasUsableWebRTCAudio(player) || hasHardStaleWebRTCPackets(player);
}

function markWebRTCPacketsRecent(feedId, player) {
    if (!player) return;
    const now = Date.now();
    player.lastPacketAt = now;
    player.lastAudioProgressAt = now;
    player.reconnectAttempts = 0;
    feedReconnectBackoffs.delete(String(feedId || ''));
    cancelWebRTCReconnect(player);
    clearPlayerTimer(player, 'connectionStateTimer');
    clearPlayerTimer(player, 'disconnectReconnectTimer');
    clearPlayerTimer(player, 'mediaEventTimer');
    clearWebRTCTrackMuteReport(feedId, player);
}

function scheduleWebRTCMediaEventStatus(feedId, player, state, message) {
    clearPlayerTimer(player, 'mediaEventTimer');
    player.mediaEventTimer = window.setTimeout(() => {
        player.mediaEventTimer = null;
        if (!isActivePlayer(feedId, player)
            || hasRecentWebRTCPackets(player)
            || (hasUsableWebRTCAudio(player) && !hasHardStaleWebRTCPackets(player))) {
            return;
        }
        const audio = player.audio;
        if (audio) {
            audio.dataset.hazePlayerState = state;
        }
        setPlayerStatus(feedId, message);
    }, WEBRTC_MEDIA_EVENT_GRACE_MS);
}

function webRTCTrackEventDetails(player) {
    const audio = player?.audio || null;
    return {
        connection_state: player?.pc?.connectionState || '',
        ice_state: player?.pc?.iceConnectionState || '',
        last_packet_age_ms: player?.lastPacketAt ? Date.now() - player.lastPacketAt : null,
        last_audio_progress_age_ms: player?.lastAudioProgressAt ? Date.now() - player.lastAudioProgressAt : null,
        last_playback_progress_age_ms: player?.lastPlaybackProgressAt ? Date.now() - player.lastPlaybackProgressAt : null,
        packet_source_age_ms: webRTCPacketSourceAgeMS(player),
        packets_recent: hasRecentWebRTCPackets(player),
        hard_stale_packets: hasHardStaleWebRTCPackets(player),
        audio_paused: audio ? audio.paused : null,
        audio_ready_state: audio ? audio.readyState : null,
        output_mixer_state: player?.audioOutputMixer?.context?.state || '',
        track_states: webRTCAudioTrackStates(player, audio),
    };
}

function webRTCAudioTrackStates(player, audio = player?.audio || null) {
    const seen = new Set();
    const tracks = [];
    const packetsRecent = hasRecentWebRTCPackets(player);
    const hardStale = hasHardStaleWebRTCPackets(player);
    const addTracks = (source, stream) => {
        for (const track of stream?.getAudioTracks?.() || []) {
            if (!track || seen.has(`${source}:${track.id}`)) continue;
            seen.add(`${source}:${track.id}`);
            tracks.push({ source, track });
        }
    };
    addTracks('audio_element', audio?.srcObject);
    addTracks('mixer_output', player?.audioOutputMixer?.outputStream);
    addTracks('mixer_source', player?.audioOutputMixer?.sourceStream);
    addTracks('remote_stream', player?.remoteStream);
    return tracks
        .map(({ source, track }) => ({
            source,
            id: track.id || '',
            ready_state: track.readyState || '',
            enabled: Boolean(track.enabled),
            raw_muted: Boolean(track.muted),
            effective_muted: Boolean(track.muted) && !packetsRecent && hardStale,
        }));
}

function scheduleWebRTCTrackMuteReport(feedId, player) {
    clearPlayerTimer(player, 'trackMuteTimer');
    player.trackMuteTimer = window.setTimeout(() => {
        player.trackMuteTimer = null;
        if (!isActivePlayer(feedId, player) || hasRecentWebRTCPackets(player) || !hasHardStaleWebRTCPackets(player)) {
            return;
        }
        player.trackMutedReported = true;
        recordWebRTCEvent(feedId, 'track_muted', webRTCTrackEventDetails(player));
        maybeFallbackWebRTCCodecToPCMU(feedId, player, 'sustained_track_mute');
    }, WEBRTC_TRACK_MUTE_GRACE_MS);
}

function clearWebRTCTrackMuteReport(feedId, player) {
    const hadPendingMute = Boolean(player?.trackMuteTimer);
    clearPlayerTimer(player, 'trackMuteTimer');
    if (!isActivePlayer(feedId, player)) return;
    if (player.trackMutedReported) {
        player.trackMutedReported = false;
        recordWebRTCEvent(feedId, 'track_unmuted', webRTCTrackEventDetails(player));
    }
}

function maybeFallbackWebRTCCodecToPCMU(feedId, player, reason) {
    if (!isActivePlayer(feedId, player) || player.mode !== 'webrtc' || player.stopping) return false;
    const requested = String(player.requestedCodec || 'auto').trim().toLowerCase();
    const negotiated = String(player.negotiatedCodec || '').trim().toLowerCase();
    if (player.codecFallbackApplied || requested !== 'auto' || negotiated !== 'g722') return false;
    player.codecFallbackApplied = true;
    const prefs = normalizedFeedPreferences(feedId);
    prefs.mode = 'webrtc';
    prefs.codec = 'pcmu';
    feedPreferences.set(feedId, prefs);
    feedCodecFallbacks.set(feedId, {
        reason,
        previous_codec: negotiated,
        requested_codec: requested,
        fallback_codec: 'pcmu',
        at: new Date().toISOString(),
    });
    const codecSelect = findFeedElement('feed-codec', feedId);
    if (codecSelect) codecSelect.value = 'pcmu';
    recordWebRTCEvent(feedId, 'codec_fallback_pcmu', {
        reason,
        previous_codec: negotiated,
        requested_codec: requested,
    });
    scheduleWebRTCReconnect(feedId, player, 'Reconnecting with PCMU audio...', { force: true });
    return true;
}

function startWebRTCStatsMonitor(feedId, player) {
    stopWebRTCStatsMonitor(player);
    if (!player?.pc?.getStats) return;
    player.statsPollTimer = window.setInterval(async () => {
        if (!isActivePlayer(feedId, player) || player.pc.connectionState === 'closed') {
            stopWebRTCStatsMonitor(player);
            return;
        }
        try {
            const statsResult = await readActiveInboundAudioStats(player.audioReceiver, player.pc, player.audioTrackId);
            const snapshot = statsResult?.snapshot || null;
            if (!snapshot) {
                if (player.pc.connectionState === 'connected') {
                    player.missingStatsPolls = (player.missingStatsPolls || 0) + 1;
                    player.stagnantStatsPolls = 0;
                    if (player.missingStatsPolls === WEBRTC_STAGNANT_STATS_POLLS) {
                        console.warn('Haze WebRTC inbound audio stats are missing.', {
                            feed_id: feedId,
                            connection_state: player.pc.connectionState,
                            ice_state: player.pc.iceConnectionState,
                            track_attached: Boolean(player.trackAttached),
                            at: new Date().toISOString(),
                        });
                        if (isActivePlayer(feedId, player) && !hasUsableWebRTCAudio(player)) {
                            setPlayerStatus(feedId, 'Waiting for audio frames...');
                        }
                    }
                    if (shouldReconnectWebRTCForMissingPackets(player) && player.missingStatsPolls >= WEBRTC_RECOVER_STATS_POLLS) {
                        if (maybeFallbackWebRTCCodecToPCMU(feedId, player, 'missing_inbound_stats')) {
                            return;
                        }
                        scheduleWebRTCReconnect(feedId, player, 'Reconnecting missing audio stream...');
                    }
                }
                return;
            }
            player.missingStatsPolls = 0;
            const previous = player.lastStats || null;
            const packetsReset = Boolean(previous && snapshot.packetsReceived < previous.packetsReceived);
            const packetsDelta = previous && !packetsReset ? snapshot.packetsReceived - previous.packetsReceived : 0;
            const bytesDelta = webRTCStatsDelta(previous, snapshot, 'bytesReceived', packetsReset);
            const concealedDelta = webRTCStatsDelta(previous, snapshot, 'concealedSamples', packetsReset);
            const silentConcealedDelta = webRTCStatsDelta(previous, snapshot, 'silentConcealedSamples', packetsReset);
            const jitterBufferEmittedDelta = webRTCStatsDelta(previous, snapshot, 'jitterBufferEmittedCount', packetsReset);
            const samplesReceivedDelta = webRTCStatsDelta(previous, snapshot, 'totalSamplesReceived', packetsReset);
            const audioEnergyDelta = webRTCStatsDelta(previous, snapshot, 'totalAudioEnergy', packetsReset);
            const audioProgress = webRTCStatsShowAudioProgress(snapshot, {
                packetsDelta,
                bytesDelta,
                concealedDelta,
                silentConcealedDelta,
                jitterBufferEmittedDelta,
                samplesReceivedDelta,
                audioEnergyDelta,
                packetsReset,
                previous,
            });
            player.lastStats = snapshot;
            window.hazeLastWebRTCStats = {
                ...(window.hazeLastWebRTCStats || {}),
                [feedId]: {
                    ...snapshot,
                    packets_delta: packetsDelta,
                    bytes_delta: bytesDelta,
                    concealed_samples_delta: concealedDelta,
                    silent_concealed_samples_delta: silentConcealedDelta,
                    jitter_buffer_emitted_delta: jitterBufferEmittedDelta,
                    total_samples_received_delta: samplesReceivedDelta,
                    total_audio_energy_delta: audioEnergyDelta,
                    audio_progress: audioProgress,
                    packets_reset: packetsReset,
                    stats_source: statsResult.source,
                    connection_state: player.pc.connectionState,
                    ice_state: player.pc.iceConnectionState,
                    packets_recent: hasRecentWebRTCPackets(player),
                    at: new Date().toISOString(),
                },
            };
            if (previous && !audioProgress && player.pc.connectionState === 'connected') {
                player.stagnantStatsPolls = (player.stagnantStatsPolls || 0) + 1;
                if (player.stagnantStatsPolls === WEBRTC_STAGNANT_STATS_POLLS) {
                    console.warn('Haze WebRTC inbound audio packets stalled.', window.hazeLastWebRTCStats[feedId]);
                    if (isActivePlayer(feedId, player) && !hasUsableWebRTCAudio(player)) {
                        setPlayerStatus(feedId, 'Waiting for audio frames...');
                    }
                }
                if (shouldReconnectWebRTCForMissingPackets(player) && player.stagnantStatsPolls >= WEBRTC_RECOVER_STATS_POLLS) {
                    console.warn('Haze WebRTC inbound audio packets stayed stalled; reconnecting.', window.hazeLastWebRTCStats[feedId]);
                    if (maybeFallbackWebRTCCodecToPCMU(feedId, player, 'stalled_inbound_packets')) {
                        return;
                    }
                    scheduleWebRTCReconnect(feedId, player, 'Reconnecting stalled audio...');
                }
            } else {
                if (audioProgress && player.audio) {
                    markWebRTCPacketsRecent(feedId, player);
                    ensureWebRTCAudioPlaying(feedId, player);
                }
                if (player.stagnantStatsPolls > 0 && isActivePlayer(feedId, player)) {
                    setHealthyWebRTCStatus(feedId, player);
                }
                player.stagnantStatsPolls = 0;
            }
        } catch (error) {
            console.warn('Unable to read Haze WebRTC stats.', error);
        }
    }, WEBRTC_STATS_INTERVAL_MS);
}

function webRTCStatsShowAudioProgress(snapshot, deltas) {
    if (!snapshot) return false;
    return Boolean(deltas?.packetsReset
        || Number(deltas?.packetsDelta || 0) > 0
        || Number(deltas?.bytesDelta || 0) > 0
        || Number(deltas?.concealedDelta || 0) > 0
        || Number(deltas?.silentConcealedDelta || 0) > 0
        || Number(deltas?.jitterBufferEmittedDelta || 0) > 0
        || Number(deltas?.samplesReceivedDelta || 0) > 0
        || Number(deltas?.audioEnergyDelta || 0) > 0
        || (!deltas?.previous && (Number(snapshot.packetsReceived || 0) > 0 || Number(snapshot.bytesReceived || 0) > 0)));
}

function stopWebRTCStatsMonitor(player) {
    if (player?.statsPollTimer) {
        window.clearInterval(player.statsPollTimer);
        player.statsPollTimer = null;
    }
}

function detachWebRTCPlayerForReconnect(feedId, player) {
    if (!isActivePlayer(feedId, player) || player.mode === 'http') return;
    clearPlayerTimer(player, 'connectionStateTimer');
    clearPlayerTimer(player, 'disconnectReconnectTimer');
    clearPlayerTimer(player, 'mediaEventTimer');
    clearPlayerTimer(player, 'trackMuteTimer');
    stopWebRTCStatsMonitor(player);
    clearPlayerInterval(player, 'playbackWatchdogTimer');
    closeWebRTCAudioOutput(player);
    player.trackAttached = false;
    player.connected = false;
    player.remoteStream = player.fallbackStream || new MediaStream();
    player.audioReceiver = null;
    player.audioTrackId = '';
    player.lastStats = null;
    player.lastPacketAt = 0;
    player.lastAudioProgressAt = 0;
    player.lastPlaybackTime = 0;
    player.lastPlaybackProgressAt = 0;
    player.stagnantStatsPolls = 0;
    player.missingStatsPolls = 0;
    player.mediaRecent = null;
    player.negotiatedCodec = '';
    player.negotiatedPayloadType = null;
    player.trackMutedReported = false;
    try {
        player.pc?.close();
    } catch {
        // Closing an already-failed peer is best-effort cleanup.
    }
    const audio = player.audio || publicWebRTCAudioElement(feedId);
    if (audio) {
        bindWebRTCAudioOutput(feedId, player, player.remoteStream, audio);
        audio.dataset.hazeTrackAttached = '0';
        audio.dataset.hazePlayerState = 'reconnecting';
    }
}

function scheduleWebRTCDisconnectReconnect(feedId, player, pc) {
    if (!isActivePlayer(feedId, player) || player.mode === 'http' || player.stopping) return;
    if (player.disconnectReconnectTimer || player.reconnectPending || player.reconnectTimer) return;
    player.disconnectReconnectTimer = window.setTimeout(() => {
        player.disconnectReconnectTimer = null;
        if (isActivePlayer(feedId, player) && player.pc === pc && pc.connectionState === 'disconnected') {
            if (!hasHardStaleWebRTCPackets(player)) {
                recordWebRTCEvent(feedId, 'disconnect_reconnect_deferred', {
                    connection_state: pc.connectionState,
                    ice_state: pc.iceConnectionState,
                    packets_recent: hasRecentWebRTCPackets(player),
                    last_audio_progress_age_ms: player.lastAudioProgressAt ? Date.now() - player.lastAudioProgressAt : null,
                    last_playback_progress_age_ms: player.lastPlaybackProgressAt ? Date.now() - player.lastPlaybackProgressAt : null,
                    packet_source_age_ms: webRTCPacketSourceAgeMS(player),
                });
                scheduleWebRTCDisconnectReconnect(feedId, player, pc);
                return;
            }
            scheduleWebRTCReconnect(feedId, player, 'Reconnecting disconnected audio...');
        }
    }, WEBRTC_DISCONNECT_GRACE_MS);
}

function scheduleWebRTCReconnect(feedId, player, reason = 'Reconnecting audio...', { force = false } = {}) {
    if (!isActivePlayer(feedId, player) || player.mode === 'http' || player.stopping) return;
    if (force) {
        clearPlayerTimer(player, 'reconnectTimer');
        player.reconnectPending = false;
    } else if (player.reconnectPending || player.reconnectTimer) {
        return;
    }
    recordWebRTCEvent(feedId, 'reconnect_scheduled', {
        reason,
        connection_state: player.pc?.connectionState || '',
        ice_state: player.pc?.iceConnectionState || '',
        track_attached: Boolean(player.trackAttached),
        has_live_track: hasLiveWebRTCAudioTrack(player),
        last_packet_age_ms: player.lastPacketAt ? Date.now() - player.lastPacketAt : null,
        last_audio_progress_age_ms: player.lastAudioProgressAt ? Date.now() - player.lastAudioProgressAt : null,
        last_playback_progress_age_ms: player.lastPlaybackProgressAt ? Date.now() - player.lastPlaybackProgressAt : null,
        packet_source_age_ms: webRTCPacketSourceAgeMS(player),
    });
    const backoffKey = String(feedId || '');
    const attempts = Math.max(0, Number(feedReconnectBackoffs.get(backoffKey) ?? player.reconnectAttempts ?? 0));
    const delay = Math.min(WEBRTC_RECONNECT_BASE_DELAY_MS * (2 ** attempts), WEBRTC_RECONNECT_MAX_DELAY_MS);
    player.reconnectAttempts = attempts + 1;
    feedReconnectBackoffs.set(backoffKey, player.reconnectAttempts);
    player.reconnectPending = true;
    player.reconnectTimer = window.setTimeout(() => {
        player.reconnectTimer = null;
        player.reconnectPending = false;
        if (!isActivePlayer(feedId, player) || player.stopping) return;
        detachWebRTCPlayerForReconnect(feedId, player);
        feedPlayers.delete(String(feedId || ''));
        startFeedWebRTC(feedId);
    }, delay);
    setPlayerStatus(feedId, reason);
}

function cancelWebRTCReconnect(player) {
    if (!player) return;
    clearPlayerTimer(player, 'reconnectTimer');
    player.reconnectPending = false;
}

async function readInboundAudioStats(source, trackId = '') {
    const report = await source.getStats();
    let selected = null;
    const expectedTrackId = String(trackId || '').trim();
    report.forEach((stats) => {
        const kind = stats.kind || stats.mediaType;
        if (stats.type !== 'inbound-rtp' || stats.isRemote || (kind && kind !== 'audio')) return;
        const statsTrackId = String(stats.trackIdentifier || '').trim();
        if (expectedTrackId && statsTrackId && statsTrackId !== expectedTrackId) return;
        const statsTimestamp = Number(stats.timestamp || 0);
        const selectedTimestamp = Number(selected?.timestamp || 0);
        if (!selected
            || statsTimestamp > selectedTimestamp
            || (statsTimestamp === selectedTimestamp && (stats.packetsReceived || 0) > (selected.packetsReceived || 0))) {
            selected = stats;
        }
    });
    if (!selected) return null;
    return {
        packetsReceived: Number(selected.packetsReceived || 0),
        bytesReceived: Number(selected.bytesReceived || 0),
        packetsLost: Number(selected.packetsLost || 0),
        jitter: Number(selected.jitter || 0),
        concealedSamples: Number(selected.concealedSamples || 0),
        silentConcealedSamples: Number(selected.silentConcealedSamples || 0),
        jitterBufferDelay: Number(selected.jitterBufferDelay || 0),
        jitterBufferEmittedCount: Number(selected.jitterBufferEmittedCount || 0),
        totalSamplesReceived: Number(selected.totalSamplesReceived || 0),
        totalAudioEnergy: Number(selected.totalAudioEnergy || 0),
        trackIdentifier: selected.trackIdentifier || '',
        timestamp: selected.timestamp || performance.now(),
    };
}

async function readActiveInboundAudioStats(receiver, peer, trackId = '') {
    if (receiver?.getStats) {
        const snapshot = await readInboundAudioStats(receiver, trackId);
        if (snapshot) return { snapshot, source: 'receiver' };
    }
    if (peer?.getStats) {
        const snapshot = await readInboundAudioStats(peer, trackId);
        if (snapshot) return { snapshot, source: 'peer' };
    }
    return null;
}

async function startFeed(feedId) {
    if (selectedFeedMode(feedId) === 'http') {
        return startFeedHTTP(feedId);
    }
    return startFeedWebRTC(feedId);
}

async function startFeedHTTP(feedId) {
    feedId = String(feedId || '');
    const feed = currentPublicFeeds.find((item) => String(item?.id || '') === feedId);
    if (!feedId || !feed || !feedCanHTTP(feed)) {
        setPlayerStatus(feedId, 'HTTP streaming unavailable');
        return;
    }
    stopFeed(feedId, { silent: true });
    const audio = findFeedElement('feed-audio', feedId);
    if (!audio) {
        return;
    }
    updateFeedModePresentation(feedId, { resetStatus: false });
    audio.autoplay = false;
    audio.controls = true;
    audio.muted = false;
    audio.playsInline = true;
    audio.dataset.hazePlayerState = 'connecting-http';
    setPlayerStatus(feedId, 'Starting HTTP audio');
    try {
        await audio.play();
        setPlayerStatus(feedId, audio.paused ? 'HTTP audio ready' : 'Playing over HTTP');
    } catch {
        audio.dataset.hazePlayerState = 'play-blocked';
        setPlayerStatus(feedId, 'Press Play in the audio control');
        setPlayerButtons(feedId, false);
    }
}

async function startFeedWebRTC(feedId) {
    feedId = String(feedId || '');
    const feed = currentPublicFeeds.find((item) => String(item?.id || '') === feedId);
    if (!feedId || !feed || !feedCanWebRTC(feed)) {
        setPlayerStatus(feedId, 'WebRTC unavailable');
        return;
    }
    const existingPlayer = feedPlayers.get(feedId);
    if (existingPlayer?.trackAttached) {
        await resumeFeedAudio(feedId, existingPlayer);
        return;
    }
    stopFeed(feedId, { silent: true });
    const audio = publicWebRTCAudioElement(feedId);
    const volume = findFeedElement('feed-volume', feedId);
    if (!audio) {
        return;
    }
    setPlayerStatus(feedId, 'Connecting...');
    setPlayerButtons(feedId, true);

    const pc = new RTCPeerConnection();
    const fallbackStream = new MediaStream();
    const player = {
        mode: 'webrtc',
        pc,
        audio,
        fallbackStream,
        remoteStream: fallbackStream,
        audioReceiver: null,
        audioTrackId: '',
        trackAttached: false,
        connected: false,
        mediaRecent: null,
        requestedCodec: DEFAULT_WEBRTC_CODEC,
        connectionStateTimer: null,
        disconnectReconnectTimer: null,
        mediaEventTimer: null,
        trackMuteTimer: null,
        trackMuteSignalCount: 0,
        trackUnmuteSignalCount: 0,
        lastTrackMuteSignalAt: 0,
        lastTrackUnmuteSignalAt: 0,
        statsPollTimer: null,
        playbackWatchdogTimer: null,
        lastStats: null,
        startedAt: Date.now(),
        lastPacketAt: 0,
        lastAudioProgressAt: 0,
        lastOutputMixerRecoveryAt: 0,
        lastPlaybackTime: 0,
        lastPlaybackProgressAt: 0,
        outputMixerRecoveryWindowAt: 0,
        outputMixerRecoveryCount: 0,
        outputMixerBypassUntil: 0,
        stagnantStatsPolls: 0,
        missingStatsPolls: 0,
        reconnectTimer: null,
        reconnectAttempts: Math.max(0, Number(feedReconnectBackoffs.get(feedId) || 0)),
        reconnectPending: false,
        codecFallbackApplied: feedCodecFallbacks.has(feedId),
        trackMutedReported: false,
        stopping: false,
    };
    feedPlayers.set(feedId, player);
    startWebRTCPlaybackWatchdog(feedId, player);

    audio.volume = Number(volume?.value ?? 1);
    audio.autoplay = true;
    audio.controls = true;
    audio.muted = false;
    audio.playsInline = true;
    audio.dataset.hazeUserPaused = '0';
    audio.dataset.hazePlayerState = 'connecting';
    audio.dataset.hazeTrackAttached = '0';
    audio.onplaying = () => {
        if (isActivePlayer(feedId, player)) {
            setHealthyWebRTCStatus(feedId, player, audio);
        }
    };
    audio.onwaiting = () => {
        if (isActivePlayer(feedId, player)) {
            if (hasRecentWebRTCPackets(player)) {
                ensureWebRTCAudioPlaying(feedId, player, audio);
                return;
            }
            scheduleWebRTCMediaEventStatus(feedId, player, 'waiting', 'Buffering...');
        }
    };
    audio.onstalled = () => {
        if (isActivePlayer(feedId, player)) {
            if (hasRecentWebRTCPackets(player)) {
                ensureWebRTCAudioPlaying(feedId, player, audio);
                return;
            }
            scheduleWebRTCMediaEventStatus(feedId, player, 'stalled', 'Audio stalled');
        }
    };
    audio.onerror = () => {
        if (isActivePlayer(feedId, player)) {
            audio.dataset.hazePlayerState = 'error';
            setPlayerStatus(feedId, 'Audio playback error');
        }
    };
    pc.addTransceiver('audio', { direction: 'recvonly' });
    pc.addEventListener('track', (event) => {
        const currentAudio = bindPlayerAudio(feedId, fallbackStream);
        if (!currentAudio) {
            setPlayerStatus(feedId, 'Audio element unavailable');
            return;
        }
        player.audioReceiver = event.receiver || null;
        player.audioTrackId = event.track.id || '';
        player.lastStats = null;
        player.stagnantStatsPolls = 0;
        player.missingStatsPolls = 0;
        player.trackAttached = true;
        player.lastAudioProgressAt = Date.now();
        currentAudio.dataset.hazeTrackAttached = '1';
        currentAudio.dataset.hazeTrackState = event.track.readyState || '';
        event.track.onmute = () => {
            if (isActivePlayer(feedId, player)) {
                markWebRTCPlaybackProgress(feedId, player, currentAudio);
                if (hasRecentWebRTCPackets(player) || !hasHardStaleWebRTCPackets(player)) {
                    ensureWebRTCAudioPlaying(feedId, player, currentAudio);
                    return;
                }
                player.trackMuteSignalCount = Number(player.trackMuteSignalCount || 0) + 1;
                player.lastTrackMuteSignalAt = Date.now();
                scheduleWebRTCTrackMuteReport(feedId, player);
            }
        };
        event.track.onunmute = () => {
            if (isActivePlayer(feedId, player)) {
                markWebRTCPlaybackProgress(feedId, player, currentAudio);
                if (!hasRecentWebRTCPackets(player)) {
                    ensureWebRTCAudioPlaying(feedId, player, currentAudio);
                    return;
                }
                if (!player.trackMuteTimer && !player.trackMutedReported) {
                    return;
                }
                player.trackUnmuteSignalCount = Number(player.trackUnmuteSignalCount || 0) + 1;
                player.lastTrackUnmuteSignalAt = Date.now();
                clearWebRTCTrackMuteReport(feedId, player);
            }
        };
        event.track.onended = () => {
            if (isActivePlayer(feedId, player)) {
                clearPlayerTimer(player, 'trackMuteTimer');
                player.trackMutedReported = false;
                recordWebRTCEvent(feedId, 'track_ended', {
                    connection_state: pc.connectionState,
                    ice_state: pc.iceConnectionState,
                });
                currentAudio.dataset.hazeTrackState = 'ended';
                scheduleWebRTCReconnect(feedId, player, 'Reconnecting ended audio track...');
            }
        };
        const stream = event.streams?.[0] || fallbackStream;
        if (!stream.getTracks().some((track) => track.id === event.track.id)) {
            stream.addTrack(event.track);
        }
        player.remoteStream = stream;
        bindWebRTCAudioOutput(feedId, player, stream, currentAudio);
        currentAudio.dataset.hazeStreamAttached = currentAudio.srcObject ? '1' : '0';
        currentAudio.dataset.hazePlayerState = 'audio-attached';
        setPlayerStatus(feedId, 'Starting audio...');
        currentAudio.play()
            .then(() => {
                if (isActivePlayer(feedId, player) && !currentAudio.paused) {
                    currentAudio.controls = true;
                    currentAudio.dataset.hazePlayerState = 'playing';
                    setPlayerStatus(feedId, 'Playing');
                } else if (isActivePlayer(feedId, player)) {
                    currentAudio.dataset.hazePlayerState = 'audio-ready';
                    setPlayerStatus(feedId, 'Audio ready');
                }
            })
            .catch(() => {
                if (isActivePlayer(feedId, player)) {
                    currentAudio.controls = true;
                    currentAudio.dataset.hazePlayerState = 'play-blocked';
                    setPlayerStatus(feedId, 'Press Play to start audio');
                    setPlayerButtons(feedId, false);
                    const stopButton = findFeedElement('feed-stop', feedId);
                    if (stopButton) stopButton.disabled = false;
                }
            });
    });
    pc.addEventListener('connectionstatechange', () => {
        if (!isActivePlayer(feedId, player)) return;
        const currentAudio = player.audio;
        recordWebRTCEvent(feedId, 'connection_state', {
            connection_state: pc.connectionState,
            ice_state: pc.iceConnectionState,
            signaling_state: pc.signalingState || '',
            track_attached: Boolean(player.trackAttached),
            has_live_track: hasLiveWebRTCAudioTrack(player),
        });
        if (currentAudio) {
            currentAudio.dataset.hazeConnectionState = pc.connectionState;
            currentAudio.dataset.hazeIceState = pc.iceConnectionState;
        }
        if (pc.connectionState === 'connected') {
            player.connected = true;
            cancelWebRTCReconnect(player);
            ensureWebRTCAudioPlaying(feedId, player, currentAudio);
            clearPlayerTimer(player, 'connectionStateTimer');
            clearPlayerTimer(player, 'disconnectReconnectTimer');
            if (currentAudio?.dataset.hazePlayerState === 'play-blocked' || currentAudio?.dataset.hazePlayerState === 'needs-play') {
                setPlayerStatus(feedId, 'Press Play to start audio');
                setPlayerButtons(feedId, false);
                const stopButton = findFeedElement('feed-stop', feedId);
                if (stopButton) stopButton.disabled = false;
            } else {
                setPlayerStatus(feedId, player.trackAttached ? 'Audio connected' : 'Connected, waiting for audio...');
                setPlayerButtons(feedId, true);
            }
        } else if (pc.connectionState === 'disconnected') {
            clearPlayerTimer(player, 'connectionStateTimer');
            player.connectionStateTimer = window.setTimeout(() => {
                if (isActivePlayer(feedId, player) && pc.connectionState === 'disconnected' && hasHardStaleWebRTCPackets(player)) {
                    setPlayerStatus(feedId, 'Reconnecting...');
                }
            }, WEBRTC_TRANSIENT_STATUS_DELAY_MS);
            scheduleWebRTCDisconnectReconnect(feedId, player, pc);
            setPlayerButtons(feedId, true);
        } else if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
            scheduleWebRTCReconnect(feedId, player, 'Reconnecting audio...');
        } else {
            setPlayerStatus(feedId, pc.connectionState);
        }
    });

    try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        await waitForIceGathering(pc);
        const local = pc.localDescription || offer;
        const offerSdp = String(local?.sdp || offer?.sdp || '').trim();
        if (!offerSdp) {
            throw new Error('Could not create WebRTC offer');
        }
        const codec = selectedFeedCodec(feedId);
        player.requestedCodec = codec || 'auto';
        const payload = {
            feed_id: feedId,
            sdp: offerSdp,
            sdp_type: local?.type || offer?.type || 'offer',
        };
        if (codec && codec !== 'auto') {
            payload.codec = codec;
        }
        const answer = await publicClient.request('webrtc_offer', {
            ...payload,
        }, 15000);
        player.mediaRecent = answer.media_recent !== false;
        player.negotiatedCodec = answer.codec || '';
        player.negotiatedPayloadType = answer.payload_type ?? null;
        recordWebRTCEvent(feedId, 'answer_received', {
            codec: player.negotiatedCodec,
            payload_type: player.negotiatedPayloadType,
            media_recent: player.mediaRecent,
        });
        await pc.setRemoteDescription({
            type: answer.sdp_type || 'answer',
            sdp: answer.sdp,
        });
        startWebRTCStatsMonitor(feedId, player);
        window.setTimeout(() => {
            const active = feedPlayers.get(feedId);
            if (active === player && !active.trackAttached) {
                setPlayerStatus(feedId, 'Connected, no audio track yet');
            }
        }, 5000);
        setPlayerStatus(feedId, player.mediaRecent ? 'Waiting for audio...' : 'Connected');
    } catch (error) {
        stopFeed(feedId, { silent: true });
        setPlayerStatus(feedId, error.message || 'Could not start stream');
    }
}

async function copyHTTPLink(feedId) {
    feedId = String(feedId || '');
    if (!feedId) return;
    const includesToken = Boolean(publicAudioToken());
    const relativeLink = httpStreamURL(feedId, false, selectedFeedCodec(feedId));
    const link = new URL(relativeLink, window.location.origin).toString();
    window.hazeLastShareLink = link;
    try {
        await copyTextToClipboard(link);
        setPlayerStatus(feedId, 'HTTP stream link copied');
    } catch {
        setPlayerStatus(feedId, includesToken ? 'Copy blocked; browser denied clipboard access' : `Copy blocked: ${link}`);
    }
}

async function copyTextToClipboard(text) {
    if (navigator.clipboard?.writeText) {
        try {
            await navigator.clipboard.writeText(text);
            return;
        } catch {
            // Fall through to the selection-based path for HTTP/LAN browsers.
        }
    }
    const input = document.createElement('textarea');
    input.value = text;
    input.setAttribute('readonly', '');
    input.className = 'public-clipboard-scratch';
    document.body.appendChild(input);
    input.focus();
    input.select();
    input.setSelectionRange(0, input.value.length);
    const copied = document.execCommand?.('copy') === true;
    input.remove();
    if (!copied) {
        throw new Error('copy command rejected');
    }
}

async function resumeFeedAudio(feedId, player) {
    const audio = bindPlayerAudio(feedId, player?.remoteStream || player?.fallbackStream || null);
    if (!audio) {
        setPlayerStatus(feedId, 'Audio element unavailable');
        return;
    }
    setPlayerButtons(feedId, true);
    audio.muted = false;
    audio.dataset.hazeUserPaused = '0';
    try {
        await audio.play();
        audio.controls = true;
        audio.dataset.hazePlayerState = audio.paused ? 'audio-ready' : 'playing';
        setPlayerStatus(feedId, audio.paused ? 'Audio ready' : 'Playing');
    } catch {
        audio.controls = true;
        audio.dataset.hazePlayerState = 'play-blocked';
        setPlayerStatus(feedId, 'Press Play to start audio');
        setPlayerButtons(feedId, false);
        const stopButton = findFeedElement('feed-stop', feedId);
        if (stopButton) stopButton.disabled = false;
    }
}

function stopFeed(feedId, { silent = false } = {}) {
    feedId = String(feedId || '');
    if (!silent) {
        feedReconnectBackoffs.delete(feedId);
    }
    const player = feedPlayers.get(feedId);
    if (player) {
        player.stopping = true;
        window.hazeLastStop = {
            feed_id: feedId,
            silent,
            mode: player.mode || 'webrtc',
            connection_state: player.pc?.connectionState || '',
            ice_state: player.pc?.iceConnectionState || '',
            signaling_state: player.pc?.signalingState || '',
            track_attached: Boolean(player.trackAttached),
            at: new Date().toISOString(),
        };
        player.pc?.close();
        clearPlayerTimer(player, 'connectionStateTimer');
        clearPlayerTimer(player, 'disconnectReconnectTimer');
        clearPlayerTimer(player, 'mediaEventTimer');
        clearPlayerTimer(player, 'trackMuteTimer');
        clearPlayerTimer(player, 'reconnectTimer');
        clearPlayerInterval(player, 'playbackWatchdogTimer');
        player.reconnectPending = false;
        player.trackMutedReported = false;
        stopWebRTCStatsMonitor(player);
        closeWebRTCAudioOutput(player);
        const audio = player.mode === 'webrtc'
            ? (player.audio || publicWebRTCAudioElement(feedId))
            : (findFeedElement('feed-audio', feedId) || player.audio);
        if (audio) {
            audio.pause();
            audio.controls = true;
            if (player.mode === 'http') {
                audio.removeAttribute('src');
                audio.load();
            } else {
                audio.srcObject = null;
            }
            audio.dataset.hazePlayerState = 'stopped';
            audio.dataset.hazeTrackAttached = '0';
            audio.dataset.hazeUserPaused = '0';
        }
        feedPlayers.delete(feedId);
    }
    if (!player) {
        pauseFeedHTTPAudio(feedId);
    }
    setPlayerButtons(feedId, false);
    if (!silent) {
        const feed = currentPublicFeeds.find((item) => String(item?.id || '') === feedId);
        const mode = player?.mode || selectedFeedMode(feedId);
        const available = Boolean(feed && (mode === 'http' ? feedCanHTTP(feed) : feedCanWebRTC(feed)));
        setPlayerStatus(feedId, available
            ? 'Stopped'
            : (mode === 'http' ? 'HTTP streaming unavailable' : 'WebRTC unavailable'));
    }
}

function isActivePlayer(feedId, player) {
    return Boolean(player && feedPlayers.get(String(feedId || '')) === player);
}

function stopRemovedPlayers(activeFeedIds) {
    Array.from(feedPlayers.keys()).forEach((feedId) => {
        if (!activeFeedIds.has(feedId)) {
            stopFeed(feedId, { silent: true });
        }
    });
}

function stopAllPlayers(options = {}) {
    Array.from(feedPlayers.keys()).forEach((feedId) => stopFeed(feedId, options));
}

function bindPlayerAudio(feedId, fallbackStream = null) {
    const player = feedPlayers.get(feedId);
    const audio = player?.mode === 'webrtc'
        ? publicWebRTCAudioElement(feedId)
        : (findFeedElement('feed-audio', feedId) || player?.audio);
    if (!player || !audio) return null;
    const volume = findFeedElement('feed-volume', feedId);
    audio.volume = Number(volume?.value ?? audio.volume ?? 1);
    const fallbackTrackCount = fallbackStream?.getAudioTracks?.().length || 0;
    const currentTrackCount = audio.srcObject?.getAudioTracks?.().length || 0;
    if (fallbackStream && fallbackTrackCount > 0 && audio.srcObject !== fallbackStream) {
        recordWebRTCEvent(feedId, 'audio_stream_bound', {
            replaced_existing_stream: Boolean(audio.srcObject),
            track_count: fallbackStream.getTracks?.().length || 0,
            current_track_count: currentTrackCount,
        });
        bindWebRTCAudioOutput(feedId, player, fallbackStream, audio);
    }
    audio.dataset.hazeStreamAttached = audio.srcObject ? '1' : '0';
    player.audio = audio;
    return audio;
}

function reattachActivePlayers() {
    for (const [feedId, player] of feedPlayers.entries()) {
        if (player.mode === 'webrtc') {
            bindPlayerAudio(feedId, player.remoteStream || player.audio?.srcObject || null);
            setPlayerButtons(feedId, true);
            setPlayerStatus(feedId, player.trackAttached ? 'Audio connected' : 'Connecting...');
            continue;
        }
        const audio = findFeedElement('feed-audio', feedId);
        if (!audio || audio === player.audio) {
            continue;
        }
        audio.volume = player.audio?.volume ?? 1;
        if (player.mode === 'http') {
            audio.srcObject = null;
            audio.src = player.httpURL || httpStreamURL(feedId);
            audio.dataset.hazePlayerState = player.audio?.dataset?.hazePlayerState || 'reattached-http';
            player.audio = audio;
            setPlayerButtons(feedId, true);
            setPlayerStatus(feedId, 'HTTP audio connected');
            audio.play().catch(() => {});
            continue;
        }
        bindWebRTCAudioOutput(feedId, player, player.remoteStream || player.audio?.srcObject || null, audio);
        audio.dataset.hazePlayerState = player.audio?.dataset?.hazePlayerState || 'reattached';
        audio.dataset.hazeTrackAttached = player.trackAttached ? '1' : '0';
        player.audio = audio;
        setPlayerButtons(feedId, true);
        setPlayerStatus(feedId, player.trackAttached ? 'Audio connected' : 'Connecting...');
        audio.play().catch(() => {});
    }
}

function waitForIceGathering(pc) {
    if (pc.iceGatheringState === 'complete') {
        return Promise.resolve();
    }
    return new Promise((resolve) => {
        const timeout = window.setTimeout(done, 3000);
        function done() {
            window.clearTimeout(timeout);
            pc.removeEventListener('icegatheringstatechange', onStateChange);
            resolve();
        }
        function onStateChange() {
            if (pc.iceGatheringState === 'complete') {
                done();
            }
        }
        pc.addEventListener('icegatheringstatechange', onStateChange);
    });
}

function normalizePublicAlerts(summary) {
    const source = summary.alerts || summary.alerts_archive_data || {};
    const byFeed = source.by_feed || {};
    const accepted = [];
    Object.entries(byFeed).forEach(([feedId, records]) => {
        publicAlertRecordList(records).forEach((record) => accepted.push({ ...record, feed_id: record.feed_id || feedId }));
    });
    return {
        accepted,
        rejected: publicAlertRecordList(source.rejected),
        expired: publicAlertRecordList(source.expired),
    };
}

function publicAlertRecordList(value) {
    if (Array.isArray(value)) {
        return value.filter(isPlainObject);
    }
    if (isPlainObject(value)) {
        if (looksLikeAlertRecord(value)) {
            return [value];
        }
        return Object.values(value).flatMap(publicAlertRecordList);
    }
    return [];
}

function looksLikeAlertRecord(value) {
    return ['id', 'identifier', 'event', 'headline', 'sent', 'expires', 'description', 'instruction', 'cap_xml_url']
        .some((key) => Object.prototype.hasOwnProperty.call(value, key));
}

function isPlainObject(value) {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function publicAlertMetaItems(record) {
    const items = [
        ['Event', publicText(record.event, 'unknown')],
        ['Feed', publicText(record.feed_id, 'none')],
        ['Status', publicText(record.status) || publicText(record.bucket, 'unknown')],
        ['Message', publicText(record.message_type, 'unknown')],
        ['Severity', publicText(record.severity, 'unknown')],
        ['Urgency', publicText(record.urgency, 'unknown')],
        ['Certainty', publicText(record.certainty, 'unknown')],
        ['Sent', formatDateTime(record.sent)],
        ['Expires', formatDateTime(record.expires)],
    ];
    if (publicText(record.reason)) items.splice(3, 0, ['Reason', publicText(record.reason)]);
    if (record.audio_url) items.push(['Audio', publicText(record.audio_mime_type, 'available')]);
    return items;
}

function groupedPublicAccepted(records) {
    const groups = new Map();
    for (const record of records) {
        const feedID = publicText(record.feed_id, 'unassigned');
        if (!groups.has(feedID)) groups.set(feedID, []);
        groups.get(feedID).push(record);
    }
    return [...groups.entries()];
}

function publicAlertAreas(record) {
    if (Array.isArray(record.areas) && record.areas.length) {
        const areas = record.areas.map((item) => publicText(item)).filter(Boolean);
        if (areas.length) return areas.join('; ');
    }
    return publicText(record.area_text) || publicText(record.sender) || 'No area text available';
}

function publicAlertCard(record) {
    const id = publicText(record.id);
    const feedID = publicText(record.feed_id);
    const headline = publicText(record.headline) || publicText(record.event, 'Weather Alert');
    const capXMLURL = safePublicLink(record.cap_xml_url, { allowRelative: true, allowHTTP: false });
    const capAudioURL = safePublicLink(record.audio_url);
    const identifier = capXMLURL
        ? `<a class="alert-card-id" href="${escapeHtml(capXMLURL)}" target="_blank" rel="noopener noreferrer" title="Open CAP XML">${escapeHtml(id)}</a>`
        : `<span class="alert-card-id">${escapeHtml(id)}</span>`;
    const meta = publicAlertMetaItems(record).map(([key, value]) => `
        <span><b>${escapeHtml(key)}</b>${escapeHtml(value)}</span>
    `).join('');
    const capAudioLink = capAudioURL ? `
        <a class="btn-action btn-link" href="${escapeHtml(capAudioURL)}" target="_blank" rel="noopener noreferrer">
            <i data-lucide="circle-play" width="13" height="13"></i>
            CAP Audio
        </a>` : '';
    const capXMLLink = capXMLURL ? `
        <a class="btn-action btn-link" href="${escapeHtml(capXMLURL)}" target="_blank" rel="noopener noreferrer">
            <i data-lucide="file-code-2" width="13" height="13"></i>
            CAP XML
        </a>` : '';
    const actions = [capAudioLink, capXMLLink].filter(Boolean).join('');
    return `
        <article class="alert-card public-alert-card" data-alert-id="${escapeHtml(id)}" data-feed-id="${escapeHtml(feedID)}">
            <div class="alert-card-main">
                <div class="alert-card-head">
                    <div>
                        <h3><span>${escapeHtml(headline)}</span>${identifier}</h3>
                        <p>${escapeHtml(publicAlertAreas(record))}</p>
                    </div>
                    <span class="alert-card-time">${escapeHtml(formatDateTime(record.updated_at || record.sent))}</span>
                </div>
                <div class="alert-card-meta">${meta}</div>
                <details class="alert-details">
                    <summary>Details</summary>
                    <div class="alert-details-grid">
                        <section>
                            <h4>Description</h4>
                            <p>${escapeHtml(publicText(record.description) || publicText(record.message) || 'No description provided.')}</p>
                        </section>
                        <section>
                            <h4>Instruction</h4>
                            <p>${escapeHtml(publicText(record.instruction, 'No instruction provided.'))}</p>
                        </section>
                    </div>
                </details>
            </div>
            <div class="alert-card-actions public-alert-card-actions">
                ${actions || '<span class="public-alert-action-note">Public details only</span>'}
            </div>
        </article>
    `;
}

function renderPublicAccepted(records) {
    const groups = groupedPublicAccepted(records);
    if (!groups.length) return '<article class="alert-empty">No accepted alerts are active for any feed.</article>';
    return groups.map(([feedID, items]) => `
        <section class="alert-feed-group">
            <div class="alert-feed-group-hd">
                <strong>${escapeHtml(feedID)}</strong>
                <span>${items.length} active</span>
            </div>
            ${items.map(publicAlertCard).join('')}
        </section>
    `).join('');
}

function renderPublicAlerts(summary) {
    if (summary.alerts_archive !== 'public' && !summary.capabilities?.public_alerts) {
        if (alertsNotice) alertsNotice.textContent = 'Public alert archive is disabled.';
        if (alertsList) alertsList.innerHTML = '';
        return;
    }
    const buckets = normalizePublicAlerts(summary);
    const records = buckets[activeAlertTab] || [];
    const signature = JSON.stringify({
        tab: activeAlertTab,
        records: records.map(publicAlertRenderSignature),
    });
    if (signature === lastAlertsSignature) {
        return;
    }
    lastAlertsSignature = signature;
    if (alertsNotice) {
        alertsNotice.textContent = records.length
            ? `${records.length} ${activeAlertTab} alert${records.length === 1 ? '' : 's'}`
            : `No ${activeAlertTab} alerts.`;
    }
    if (!alertsList) {
        return;
    }
    alertsList.innerHTML = activeAlertTab === 'accepted'
        ? renderPublicAccepted(records)
        : (records.length
            ? records.map(publicAlertCard).join('')
            : `<article class="alert-empty">No ${escapeHtml(activeAlertTab)} alerts are archived.</article>`);
    window.lucide?.createIcons();
}

function publicAlertRenderSignature(record) {
    return [
        record.id || '',
        record.feed_id || '',
        record.bucket || '',
        record.status || '',
        record.reason || '',
        record.updated_at || '',
        record.sent || '',
        record.expires || '',
        publicText(record.message_type),
        publicText(record.headline),
        publicText(record.event),
        publicText(record.severity),
        publicText(record.urgency),
        publicText(record.certainty),
        publicAlertAreas(record),
        publicText(record.description) || publicText(record.message),
        publicText(record.instruction),
        record.audio_url || '',
        record.audio_mime_type || '',
        record.cap_xml_url || '',
    ];
}

function requestedListenFeedID(feeds) {
    const params = new URLSearchParams(window.location.search);
    const requested = params.get('feed') || params.get('feed_id') || '';
    if (requested && feeds.some((feed) => String(feed.id || '') === requested)) {
        return requested;
    }
    return String(feeds.find(feedCanHTTP)?.id || feeds[0]?.id || '');
}

function requestedListenCodec() {
    const params = new URLSearchParams(window.location.search);
    return normalizeHTTPCodec(params.get('codec') || params.get('format') || publicFeedGlobalPreferences.codec || DEFAULT_HTTP_CODEC);
}

function webRTCOutputMixerEnabled() {
    const params = new URLSearchParams(window.location.search);
    const value = String(params.get('webrtcMixer') || params.get('audioMixer') || '').trim().toLowerCase();
    return ['1', 'true', 'on', 'yes', 'mixer', 'mixed'].includes(value);
}

function webRTCOutputMixerDisabled() {
    return !webRTCOutputMixerEnabled();
}

function destroyListenMap() {
    listenMapAbort?.abort();
    listenMapAbort = null;
    listenAlertAbort?.abort();
    listenAlertAbort = null;
    if (listenAlertReloadTimer) window.clearTimeout(listenAlertReloadTimer);
    listenAlertReloadTimer = null;
    listenMapPopup?.remove();
    listenMapPopup = null;
    listenMap?.remove();
    listenMap = null;
}

function feedMapURL(feedID, alerts = false, bounds = null) {
    const params = new URLSearchParams({ feed: feedID });
    if (alerts) {
        params.set('alerts', '1');
        if (bounds) params.set('bbox', [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()].join(','));
    }
    return `${API_BASE}/feed/map?${params.toString()}`;
}

function fallbackFeedMapStyle() {
    return {
        version: 8,
        sources: {},
        layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#071018' } }],
    };
}

function validPublicBasemapProfile(profile) {
    try {
        const styleURL = new URL(String(profile?.style_url || ''));
        const satelliteURL = new URL(String(profile?.satellite?.tile_url || ''));
        return styleURL.protocol === 'https:'
            && styleURL.hostname === 'tiles.openfreemap.org'
            && styleURL.pathname === '/styles/dark'
            && !styleURL.search
            && !styleURL.hash
            && satelliteURL.protocol === 'https:'
            && satelliteURL.hostname === 'server.arcgisonline.com'
            && decodeURIComponent(satelliteURL.pathname).toLowerCase() === '/arcgis/rest/services/world_imagery/mapserver/tile/{z}/{y}/{x}'
            && !satelliteURL.search
            && !satelliteURL.hash
            && Number(profile?.satellite?.max_zoom) >= 1
            && Number(profile?.satellite?.max_zoom) <= 20;
    } catch {
        return false;
    }
}

async function fetchPublicBasemapProfile(signal) {
    const response = await fetch(PUBLIC_BASEMAP_PROFILE_URL, {
        method: 'GET',
        credentials: 'same-origin',
        headers: session.authHeaders({ Accept: 'application/json' }),
        signal,
    });
    if (!response.ok) return null;
    const profile = await response.json();
    return validPublicBasemapProfile(profile) ? profile : null;
}

async function fetchFeedMap(feedID, alerts, signal, bounds = null) {
    const response = await fetch(feedMapURL(feedID, alerts, bounds), {
        method: 'GET',
        credentials: 'same-origin',
        headers: session.authHeaders({ Accept: 'application/json' }),
        signal,
    });
    if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || `Location map request failed: ${response.status}`);
    }
    return response.json();
}

function locationCoordinateText(location) {
    const latitude = Number(location?.latitude);
    const longitude = Number(location?.longitude);
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return 'Unavailable';
    return `${latitude.toFixed(4)}, ${longitude.toFixed(4)}`;
}

function locationCodeValues(...values) {
    const codes = new Set();
    const add = (value) => {
        if (typeof value === 'string') {
            const code = value.trim();
            if (code) codes.add(code);
        } else if (Array.isArray(value)) {
            value.forEach(add);
        }
    };
    values.forEach(add);
    return [...codes].sort((left, right) => left.localeCompare(right));
}

async function copyLocationValue(value, button, label) {
    let copied = false;
    try {
        if (navigator.clipboard?.writeText) {
            await navigator.clipboard.writeText(value);
            copied = true;
        }
    } catch {
        // The fallback below covers browsers that deny the Clipboard API here.
    }
    if (!copied) {
        const fallback = document.createElement('textarea');
        fallback.value = value;
        fallback.setAttribute('readonly', '');
        fallback.style.position = 'fixed';
        fallback.style.opacity = '0';
        document.body.append(fallback);
        fallback.select();
        try {
            copied = document.execCommand('copy');
        } finally {
            fallback.remove();
        }
    }
    const originalLabel = `Copy ${label}`;
    button.textContent = copied ? '✓' : '!';
    button.dataset.copyState = copied ? 'copied' : 'failed';
    button.setAttribute('aria-label', copied ? `Copied ${label}` : `Could not copy ${label}`);
    button.title = copied ? `Copied ${label}` : `Could not copy ${label}`;
    window.setTimeout(() => {
        button.textContent = '⧉';
        button.dataset.copyState = '';
        button.setAttribute('aria-label', originalLabel);
        button.title = originalLabel;
    }, 1400);
}

function locationCopyButton(value, label) {
    const button = document.createElement('button');
    const copyLabel = `Copy ${label}`;
    button.type = 'button';
    button.className = 'public-location-copy-button';
    button.dataset.locationCopyValue = value;
    button.setAttribute('aria-label', copyLabel);
    button.title = copyLabel;
    button.textContent = '⧉';
    button.addEventListener('click', () => {
        void copyLocationValue(value, button, label);
    });
    return button;
}

function appendLocationCode(cell, labelText, code) {
    const pair = document.createElement('span');
    pair.className = 'public-location-code-pair';
    pair.dataset.locationCode = code;
    const value = document.createElement('span');
    value.className = 'public-location-code-value';
    value.textContent = code;
    pair.append(value, locationCopyButton(code, `${labelText} code ${code}`));
    cell.append(pair);
}

function appendLocationCodeColumn(cell, label, values) {
    cell.classList.add('public-location-code-cell');
    if (!values.length) {
        cell.textContent = 'Unavailable';
        return;
    }
    for (const code of values) appendLocationCode(cell, label, code);
}

function appendLocationCoordinates(cell, location) {
    cell.classList.add('public-location-coordinate-cell');
    const coordinateText = locationCoordinateText(location);
    if (coordinateText === 'Unavailable') {
        cell.textContent = coordinateText;
        return;
    }
    const coordinate = document.createElement('span');
    coordinate.className = 'public-location-coordinate';
    coordinate.dataset.locationCoordinate = coordinateText;
    const value = document.createElement('span');
    value.className = 'public-location-coordinate-value';
    value.textContent = coordinateText;
    coordinate.append(value, locationCopyButton(coordinateText, `coordinates ${coordinateText}`));
    cell.append(coordinate);
}

function feedMapLocationColumns(locations) {
    const countries = new Set(locations.map((location) => String(location?.country || '').toUpperCase()));
    const hasSGC = countries.has('CA') || locations.some((location) => locationCodeValues(location?.sgc_codes).length);
    const hasUSCodes = countries.has('US') || locations.some((location) => locationCodeValues(
        location?.fips_codes,
        location?.nws_county_codes,
        location?.nws_zone_codes,
    ).length);
    const columns = [
        { key: 'name', label: 'Location Name', type: 'name' },
        {
            key: 'same',
            label: 'SAME',
            type: 'code',
            values: (location) => locationCodeValues(location?.same_codes, location?.same_code),
        },
    ];
    if (hasSGC) {
        columns.push({
            key: 'sgc',
            label: 'SGC',
            type: 'code',
            values: (location) => locationCodeValues(location?.sgc_codes),
        });
    }
    if (hasUSCodes) {
        columns.push(
            {
                key: 'fips',
                label: 'FIPS',
                type: 'code',
                values: (location) => locationCodeValues(location?.fips_codes),
            },
            {
                key: 'nws-county',
                label: 'NWS County',
                type: 'code',
                values: (location) => locationCodeValues(location?.nws_county_codes),
            },
            {
                key: 'nws-zone',
                label: 'NWS Zone',
                type: 'code',
                values: (location) => locationCodeValues(location?.nws_zone_codes),
            },
        );
    }
    columns.push({ key: 'coordinates', label: 'Lat/Lon', type: 'coordinates' });
    return columns;
}

function renderFeedMapLocationHeader(columns) {
    const head = listenPanel?.querySelector('[data-feed-location-head]');
    if (!head) return;
    const row = document.createElement('tr');
    for (const column of columns) {
        const header = document.createElement('th');
        header.scope = 'col';
        header.dataset.locationColumn = column.key;
        header.textContent = column.label;
        row.append(header);
    }
    head.replaceChildren(row);
}

function renderFeedMapLocations(locations) {
    const body = listenPanel?.querySelector('[data-feed-location-rows]');
    if (!body) return new Map();
    const locationItems = Array.isArray(locations) ? locations : [];
    const columns = feedMapLocationColumns(locationItems);
    renderFeedMapLocationHeader(columns);
    body.replaceChildren();
    const rows = new Map();
    for (const location of locationItems) {
        const row = document.createElement('tr');
        row.dataset.locationKey = String(location.key || '');
        for (const column of columns) {
            const cell = document.createElement('td');
            cell.dataset.locationColumn = column.key;
            if (column.type === 'name') {
                cell.textContent = location.name || 'Unnamed location';
            } else if (column.type === 'code') {
                appendLocationCodeColumn(cell, column.label, column.values(location));
            } else {
                appendLocationCoordinates(cell, location);
            }
            row.append(cell);
        }
        body.append(row);
        rows.set(row.dataset.locationKey, row);
    }
    if (!body.children.length) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = columns.length;
        cell.className = 'public-location-empty';
        cell.textContent = 'No mapped CLCs or counties are configured for this feed.';
        row.append(cell);
        body.append(row);
    }
    return rows;
}

function geometryBounds(featureCollection) {
    const bounds = new window.maplibregl.LngLatBounds();
    let populated = false;
    const visit = (coordinates) => {
        if (!Array.isArray(coordinates)) return;
        if (coordinates.length >= 2 && Number.isFinite(Number(coordinates[0])) && Number.isFinite(Number(coordinates[1]))) {
            bounds.extend([Number(coordinates[0]), Number(coordinates[1])]);
            populated = true;
            return;
        }
        coordinates.forEach(visit);
    };
    for (const feature of featureCollection?.features || []) visit(feature?.geometry?.coordinates);
    return populated ? bounds : null;
}

function alertFillExpression() {
    return [
        'match', ['downcase', ['coalesce', ['get', 'severity'], 'unknown']],
        'extreme', '#991b1b',
        'severe', '#dc2626',
        'moderate', '#f59e0b',
        'minor', '#facc15',
        '#a855f7',
    ];
}

function addDetailedBasemapContextLayers(map) {
    const source = 'openmaptiles';
    if (!map.getSource(source)) return;

    const styleLayers = map.getStyle()?.layers || [];
    for (const layer of styleLayers) {
        if (layer.source === source && layer['source-layer'] === 'boundary' && layer.type === 'line') {
            map.setLayoutProperty(layer.id, 'visibility', 'none');
        }
        if (layer.source === source && layer['source-layer'] === 'place' && layer.type === 'symbol') {
            map.setPaintProperty(layer.id, 'text-color', '#f1f5f9');
            map.setPaintProperty(layer.id, 'text-halo-color', 'rgba(3, 8, 18, 0.96)');
            map.setPaintProperty(layer.id, 'text-halo-width', 1.45);
        }
    }

    const firstPlaceLabel = styleLayers.find((layer) => layer.source === source && layer['source-layer'] === 'place' && layer.type === 'symbol')?.id;
    const addLayer = (layer) => {
        if (!map.getLayer(layer.id)) map.addLayer(layer, firstPlaceLabel);
    };
    const boundaryFilter = (levels) => [
        'all',
        ['in', ['to-string', ['get', 'admin_level']], ['literal', levels.map(String)]],
        ['!=', ['to-string', ['coalesce', ['get', 'maritime'], 0]], '1'],
    ];

    addLayer({
        id: 'haze-context-country-boundaries',
        type: 'line',
        source,
        'source-layer': 'boundary',
        minzoom: 1,
        filter: boundaryFilter([2]),
        paint: {
            'line-color': 'rgba(226, 232, 240, 0.86)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 1, 1.05, 7, 2.15],
        },
    });
    addLayer({
        id: 'haze-context-state-boundaries',
        type: 'line',
        source,
        'source-layer': 'boundary',
        minzoom: 3,
        filter: boundaryFilter([4]),
        paint: {
            'line-color': 'rgba(148, 163, 184, 0.78)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.75, 10, 1.55],
        },
    });
    addLayer({
        id: 'haze-context-municipal-boundaries',
        type: 'line',
        source,
        'source-layer': 'boundary',
        minzoom: 5.5,
        filter: boundaryFilter([6, 7, 8, 9, 10]),
        paint: {
            'line-color': 'rgba(125, 211, 252, 0.48)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 5.5, 0.55, 13, 1.25],
            'line-dasharray': [2.5, 1.5],
        },
    });
    addLayer({
        id: 'haze-context-border-inspect',
        type: 'line',
        source,
        'source-layer': 'boundary',
        minzoom: 3,
        filter: boundaryFilter([2, 4, 6, 7, 8, 9, 10]),
        layout: { visibility: 'none' },
        paint: {
            'line-color': 'rgba(196, 181, 253, 0.46)',
            'line-width': ['interpolate', ['linear'], ['zoom'], 3, 1, 13, 1.8],
        },
    });
    addLayer({
        id: 'haze-context-border-hover',
        type: 'line',
        source,
        'source-layer': 'boundary',
        minzoom: 3,
        filter: ['==', ['get', 'name'], '__haze_no_border__'],
        layout: { visibility: 'none' },
        paint: {
            'line-color': '#fbbf24',
            'line-width': ['interpolate', ['linear'], ['zoom'], 3, 2.2, 13, 4],
        },
    });

    for (const layer of styleLayers) {
        if (layer.source === source && layer['source-layer'] === 'place' && layer.type === 'symbol' && map.getLayer(layer.id)) {
            map.moveLayer(layer.id);
        }
    }
}

function readableMapProperty(key) {
    const labels = {
        admin_level: 'Admin level', alert_id: 'Alert ID', area_description: 'Area', certainty: 'Certainty',
        code: 'Source code', country: 'Country', effective: 'Effective', event: 'Event', expires: 'Expires',
        fips_codes: 'FIPS', geometry_method: 'Geometry', headline: 'Headline', latitude: 'Latitude',
        location_name: 'Location', longitude: 'Longitude', message_type: 'Message type', nws_county_codes: 'NWS county',
        nws_zone_codes: 'NWS zone', onset: 'Onset', provider_version: 'Dataset', same_code: 'SAME',
        same_codes: 'SAME codes', sender: 'Sender', sender_name: 'Sender name', severity: 'Severity', sgc_codes: 'SGC',
        source: 'Source', status: 'Status', urgency: 'Urgency', class: 'Class', disputed: 'Disputed', maritime: 'Maritime',
    };
    return labels[key] || key.replaceAll('_', ' ').replace(/^./, (letter) => letter.toUpperCase());
}

function mapPopupContent(title, properties, excluded = []) {
    const content = document.createElement('div');
    const heading = document.createElement('strong');
    heading.textContent = String(title || 'Map feature');
    content.append(heading);
    const omitted = new Set(excluded);
    const entries = Object.entries(properties || {})
        .filter(([key, value]) => !omitted.has(key) && value !== null && value !== undefined && String(value).trim() !== '')
        .sort(([left], [right]) => readableMapProperty(left).localeCompare(readableMapProperty(right)));
    if (entries.length) {
        const details = document.createElement('dl');
        for (const [key, value] of entries) {
            const term = document.createElement('dt');
            const description = document.createElement('dd');
            term.textContent = readableMapProperty(key);
            description.textContent = String(value);
            details.append(term, description);
        }
        content.append(details);
    }
    return content;
}

function borderHoverFilter(properties) {
    const filters = [];
    for (const key of ['admin_level', 'name', 'name_en', 'name:en']) {
        const value = properties?.[key];
        if (value !== undefined && value !== null && String(value) !== '') filters.push(['==', ['to-string', ['get', key]], String(value)]);
    }
    return filters.length ? ['all', ...filters] : ['==', ['get', 'name'], '__haze_no_border__'];
}

function addSatelliteBasemapLayer(map, profile) {
    const satellite = profile?.satellite;
    if (!satellite?.tile_url || map.getSource('haze-satellite')) return false;
    map.addSource('haze-satellite', {
        type: 'raster',
        tiles: [satellite.tile_url],
        tileSize: 256,
        maxzoom: Number(satellite.max_zoom || 19),
        attribution: satellite.attribution || '',
    });
    const firstLabel = (map.getStyle()?.layers || []).find((layer) => layer.type === 'symbol')?.id;
    map.addLayer({
        id: 'haze-satellite-layer',
        type: 'raster',
        source: 'haze-satellite',
        layout: { visibility: 'none' },
        paint: { 'raster-fade-duration': 0, 'raster-contrast': 0.12, 'raster-saturation': -0.08 },
    }, firstLabel);
    return true;
}

function addBasemapModeControl(map) {
    const container = document.createElement('div');
    container.className = 'maplibregl-ctrl maplibregl-ctrl-group haze-basemap-control';
    const buttons = new Map();
    const selectMode = (mode) => {
        const satellite = mode === 'satellite';
        if (map.getLayer('haze-satellite-layer')) {
            map.setLayoutProperty('haze-satellite-layer', 'visibility', satellite ? 'visible' : 'none');
        }
        for (const [candidate, button] of buttons.entries()) {
            const active = candidate === mode;
            button.dataset.active = String(active);
            button.setAttribute('aria-pressed', String(active));
        }
    };
    for (const [mode, label] of [['vector', 'Vector'], ['satellite', 'Satellite']]) {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = label;
        button.title = `Show ${label.toLowerCase()} basemap`;
        button.setAttribute('aria-label', button.title);
        button.addEventListener('click', () => selectMode(mode));
        buttons.set(mode, button);
        container.append(button);
    }
    selectMode('vector');
    map.addControl({
        onAdd: () => container,
        onRemove: () => container.remove(),
    }, 'top-left');
}

function setFeedMapStatus(text, state = 'pending') {
    const status = listenPanel?.querySelector('[data-feed-map-status]');
    if (!status) return;
    status.textContent = text;
    status.dataset.state = state;
}

async function initializeFeedMap(feedID) {
    destroyListenMap();
    const mapElement = listenPanel?.querySelector('[data-feed-map]');
    const alertToggle = listenPanel?.querySelector('[data-feed-alert-toggle]');
    const borderToggle = listenPanel?.querySelector('[data-feed-border-toggle]');
    if (!mapElement || !window.maplibregl) {
        setFeedMapStatus('MapLibre is unavailable in this browser.', 'err');
        return;
    }
    listenMapAbort = new AbortController();
    const signal = listenMapAbort.signal;
    if (alertToggle) alertToggle.disabled = true;
    if (borderToggle) borderToggle.disabled = true;
    try {
        setFeedMapStatus('Loading served locations from haze-location...', 'pending');
        const [payload, basemapProfile] = await Promise.all([
            fetchFeedMap(feedID, false, signal),
            fetchPublicBasemapProfile(signal).catch(() => null),
        ]);
        if (signal.aborted) return;
        const rows = renderFeedMapLocations(payload.locations);
        const servedCount = Array.isArray(payload.locations) ? payload.locations.length : 0;
        const servedStatus = () => `${servedCount} served location${servedCount === 1 ? '' : 's'} from catalog ${payload.catalog_generation || 'unknown'}.`;
        window.maplibregl.setWorkerUrl('/assets/vendor/maplibre/maplibre-gl-csp-worker.js?v=5.24.0');
        let mapLayersAttached = false;
        let basemapFallbackAttempted = !basemapProfile;
        listenMap = new window.maplibregl.Map({
            container: mapElement,
            attributionControl: false,
            center: [-106, 54],
            zoom: 2.2,
            minZoom: 1,
            maxZoom: 14,
            maxPitch: 0,
            pitchWithRotate: false,
            renderWorldCopies: false,
            fadeDuration: 0,
            style: basemapProfile?.style_url || fallbackFeedMapStyle(),
        });
        listenMap.addControl(new window.maplibregl.NavigationControl({ showCompass: false }), 'top-right');
        listenMap.addControl(new window.maplibregl.AttributionControl({
            compact: true,
            customAttribution: basemapProfile?.attribution
                ? `Boundaries: ECCC and NOAA/NWS. ${basemapProfile.attribution}`
                : 'Boundaries: ECCC and NOAA/NWS',
        }));
        const attachMapLayers = () => {
            if (!listenMap || signal.aborted || mapLayersAttached || !listenMap.isStyleLoaded()) return;
            mapLayersAttached = true;
            listenMap.addSource('served-locations', { type: 'geojson', data: payload.coverage });
            listenMap.addLayer({
                id: 'served-location-fill', type: 'fill', source: 'served-locations',
                paint: { 'fill-color': '#06b6d4', 'fill-opacity': 0.38 },
            });
            listenMap.addLayer({
                id: 'served-location-outline', type: 'line', source: 'served-locations',
                paint: { 'line-color': '#38bdf8', 'line-width': 1.9 },
            });
            listenMap.addLayer({
                id: 'served-location-hover', type: 'line', source: 'served-locations',
                filter: ['==', ['get', 'key'], ''],
                paint: { 'line-color': '#fbbf24', 'line-width': 4 },
            });
            listenMap.addSource('active-alerts', { type: 'geojson', data: payload.alerts });
            listenMap.addLayer({
                id: 'active-alert-fill', type: 'fill', source: 'active-alerts',
                paint: { 'fill-color': alertFillExpression(), 'fill-opacity': 0.42 },
            });
            listenMap.addLayer({
                id: 'active-alert-outline', type: 'line', source: 'active-alerts',
                paint: { 'line-color': '#fecaca', 'line-width': 2.6 },
            });
            const hasSatellite = addSatelliteBasemapLayer(listenMap, basemapProfile);
            addDetailedBasemapContextLayers(listenMap);
            if (hasSatellite) addBasemapModeControl(listenMap);
            const bounds = geometryBounds(payload.coverage);
            if (bounds) listenMap.fitBounds(bounds, { padding: 36, maxZoom: 9, duration: 0 });
            let activeRow = null;
            listenMap.on('mousemove', 'served-location-fill', (event) => {
                const feature = event.features?.[0];
                const properties = feature?.properties || {};
                const key = String(properties.key || '');
                listenMap.getCanvas().style.cursor = 'pointer';
                listenMap.setFilter('served-location-hover', ['==', ['get', 'key'], key]);
                activeRow?.classList.remove('map-hover');
                activeRow = rows.get(key) || null;
                activeRow?.classList.add('map-hover');
                activeRow?.scrollIntoView({ block: 'nearest' });
                listenMapPopup ||= new window.maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12, anchor: 'bottom', className: 'public-location-popup' });
                listenMapPopup
                    .setLngLat(event.lngLat)
                    .setDOMContent(mapPopupContent(properties.location_name || 'Unnamed location', properties, ['key', 'location_name']))
                    .addTo(listenMap);
            });
            listenMap.on('mouseleave', 'served-location-fill', () => {
                listenMap.getCanvas().style.cursor = '';
                listenMap.setFilter('served-location-hover', ['==', ['get', 'key'], '']);
                activeRow?.classList.remove('map-hover');
                activeRow = null;
                listenMapPopup?.remove();
            });

            listenMap.on('mousemove', 'haze-context-border-inspect', (event) => {
                if (!borderToggle?.checked) return;
                const properties = event.features?.[0]?.properties || {};
                listenMap.getCanvas().style.cursor = 'crosshair';
                listenMap.setFilter('haze-context-border-hover', borderHoverFilter(properties));
                const title = properties.name_en || properties['name:en'] || properties.name || 'Administrative boundary';
                listenMapPopup ||= new window.maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12, anchor: 'bottom', className: 'public-location-popup' });
                listenMapPopup.setLngLat(event.lngLat).setDOMContent(mapPopupContent(title, properties, ['name', 'name_en', 'name:en'])).addTo(listenMap);
            });
            listenMap.on('mouseleave', 'haze-context-border-inspect', () => {
                if (listenMap?.getLayer('haze-context-border-hover')) {
                    listenMap.setFilter('haze-context-border-hover', ['==', ['get', 'name'], '__haze_no_border__']);
                }
                listenMap.getCanvas().style.cursor = '';
                listenMapPopup?.remove();
            });
            borderToggle?.addEventListener('change', () => {
                const visibility = borderToggle.checked ? 'visible' : 'none';
                listenMap.setLayoutProperty('haze-context-border-inspect', 'visibility', visibility);
                listenMap.setLayoutProperty('haze-context-border-hover', 'visibility', visibility);
                if (!borderToggle.checked) {
                    listenMap.setFilter('haze-context-border-hover', ['==', ['get', 'name'], '__haze_no_border__']);
                    listenMapPopup?.remove();
                }
            });

            listenMap.on('mousemove', 'active-alert-fill', (event) => {
                const properties = event.features?.[0]?.properties || {};
                listenMap.getCanvas().style.cursor = 'pointer';
                listenMapPopup ||= new window.maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12, anchor: 'bottom', className: 'public-location-popup' });
                listenMapPopup
                    .setLngLat(event.lngLat)
                    .setDOMContent(mapPopupContent(properties.headline || properties.event || 'Alert in effect', properties, ['headline']))
                    .addTo(listenMap);
            });
            listenMap.on('mouseleave', 'active-alert-fill', () => {
                listenMap.getCanvas().style.cursor = '';
                listenMapPopup?.remove();
            });

            setFeedMapStatus(servedStatus(), servedCount ? 'ok' : 'warn');
            if (alertToggle) alertToggle.disabled = false;
            if (borderToggle) borderToggle.disabled = false;
        };
        listenMap.on('load', attachMapLayers);
        listenMap.on('style.load', attachMapLayers);
        listenMap.on('error', () => {
            if (!listenMap || signal.aborted || mapLayersAttached) return;
            if (basemapProfile && !basemapFallbackAttempted) {
                basemapFallbackAttempted = true;
                listenMap.setStyle(fallbackFeedMapStyle());
                setFeedMapStatus('Detailed basemap unavailable. Showing served locations only.', 'warn');
                return;
            }
            setFeedMapStatus('The served-location map could not be rendered.', 'err');
        });
        const loadVisibleAlerts = async () => {
            if (!listenMap?.getSource('active-alerts')) return;
            listenAlertAbort?.abort();
            const controller = new AbortController();
            listenAlertAbort = controller;
            setFeedMapStatus('Loading alerts in effect for this map view...', 'pending');
            try {
                const alertPayload = await fetchFeedMap(feedID, true, controller.signal, listenMap.getBounds());
                if (controller.signal.aborted || signal.aborted || !listenMap?.getSource('active-alerts')) return;
                listenMap.getSource('active-alerts').setData(alertPayload.alerts);
                const count = Number(alertPayload.alert_count || 0);
                const truncated = alertPayload.alert_truncated ? ' The result limit was reached.' : '';
                setFeedMapStatus(count ? `${count} active alert area${count === 1 ? '' : 's'} in this map view.${truncated}` : 'No alerts are in effect in this map view.', count ? 'warn' : 'ok');
            } catch (error) {
                if (error.name !== 'AbortError') {
                    setFeedMapStatus(error.message || 'Alerts could not be loaded.', 'err');
                    alertToggle.checked = false;
                }
            } finally {
                if (listenAlertAbort === controller) listenAlertAbort = null;
            }
        };
        alertToggle?.addEventListener('change', () => {
            if (!listenMap?.getSource('active-alerts')) return;
            if (!alertToggle.checked) {
                listenAlertAbort?.abort();
                listenAlertAbort = null;
                listenMap.getSource('active-alerts').setData({ type: 'FeatureCollection', features: [] });
                setFeedMapStatus(servedStatus(), servedCount ? 'ok' : 'warn');
                return;
            }
            void loadVisibleAlerts();
        });
        listenMap.on('moveend', () => {
            if (!alertToggle?.checked) return;
            if (listenAlertReloadTimer) window.clearTimeout(listenAlertReloadTimer);
            listenAlertReloadTimer = window.setTimeout(() => void loadVisibleAlerts(), 180);
        });
    } catch (error) {
        if (error.name !== 'AbortError') setFeedMapStatus(error.message || 'Served locations could not be loaded.', 'err');
        if (alertToggle) alertToggle.disabled = false;
        if (borderToggle) borderToggle.disabled = false;
    }
}

function renderListen(feeds) {
    if (!listenNotice || !listenPanel) return;
    currentPublicFeeds = feeds || [];
    if (summaryState?.feeds_access === 'disabled') {
        destroyListenMap();
        listenNotice.textContent = 'Public feeds are disabled on this system.';
        listenPanel.innerHTML = '';
        return;
    }
    const feedID = requestedListenFeedID(feeds || []);
    const feed = (feeds || []).find((item) => String(item.id || '') === feedID);
    if (!feed) {
        destroyListenMap();
        listenNotice.textContent = 'No public feed was found for this link.';
        listenPanel.innerHTML = '';
        return;
    }
    const webRTCAvailable = feedCanWebRTC(feed);
    const httpAvailable = feedCanHTTP(feed);
    const canPlay = webRTCAvailable || httpAvailable;
    const preferences = normalizedFeedPreferences(feedID, feed);
    const mode = preferences.mode === 'webrtc' && webRTCAvailable ? 'webrtc' : 'http';
    const codec = mode === 'http' ? requestedListenCodec() : preferences.codec;
    const tx = feed.transmitter || {};
    const siteNames = (tx.site_names || [tx.site_name]).filter(Boolean).join(', ') || feed.name || 'Unnamed site';
    const nowPlaying = publicNowPlaying(feed.runtime?.now_playing);
    const listenSignature = JSON.stringify({
        feedID,
        codec,
        media: Boolean(summaryState?.media_available),
        siteNames,
        mode,
        webrtc: Boolean(feed.webrtc_enabled),
        http: Boolean(feed.http_stream_enabled),
    });
    if (listenSignature === lastListenSignature) {
        const nowPlayingElement = listenPanel.querySelector('[data-listen-now-playing]');
        if (nowPlayingElement && nowPlayingElement.textContent !== nowPlaying) {
            nowPlayingElement.textContent = nowPlaying;
        }
        return;
    }
    lastListenSignature = listenSignature;
    const activePlayer = feedPlayers.get(feedID);
    if (activePlayer && (!canPlay || activePlayer.mode !== mode)) {
        stopFeed(feedID, { silent: true });
    }
    const streamURL = httpAvailable ? httpStreamURL(feedID, false, codec) : '';
    listenNotice.textContent = !canPlay
        ? 'Feed information is available. Streaming is not configured for this feed.'
        : (mode === 'webrtc' ? 'Public WebRTC listener ready.' : 'Public HTTP listener ready.');
    const titleMarkup = `
        <div class="public-listen-title">
            <p class="feed-id">${escapeHtml(feedID)}</p>
            <h3>${escapeHtml(siteNames)}</h3>
            <p class="public-feed-now" data-listen-now-playing>${escapeHtml(nowPlaying)}</p>
        </div>
    `;
    let feedInfoMarkup = `
        <article class="feed-card public-listen-card public-listen-info-card">
            ${titleMarkup}
            <p class="public-listen-stream-unavailable">Streaming is not configured for this feed.</p>
        </article>
    `;
    if (mode === 'webrtc' && webRTCAvailable) {
        feedInfoMarkup = `
            <article class="feed-card public-listen-card public-listen-player-card" data-feed-card="${escapeHtml(feedID)}">
                ${titleMarkup}
                <div class="public-listen-toolbar">
                    <label class="public-listen-field">
                        <span>Codec</span>
                        <select data-listen-webrtc-codec>
                            ${WEBRTC_CODECS.map(([value, label]) => `
                                <option value="${value}" ${codec === value ? 'selected' : ''}>${label}</option>
                            `).join('')}
                        </select>
                    </label>
                    <button class="btn-action public-player-btn" type="button" data-feed-play="${escapeHtml(feedID)}" data-feed-playable="1">
                        <i data-lucide="play" width="14" height="14"></i>
                        <span>Listen</span>
                    </button>
                    <button class="btn-action public-player-btn" type="button" data-feed-stop="${escapeHtml(feedID)}" disabled>
                        <i data-lucide="square" width="14" height="14"></i>
                        <span>Stop</span>
                    </button>
                </div>
                <audio class="public-listen-audio" data-feed-audio="${escapeHtml(feedID)}" data-feed-http-enabled="0" controls playsinline preload="none"></audio>
                <p class="public-player-status" data-feed-status="${escapeHtml(feedID)}">Ready over WebRTC</p>
            </article>
        `;
    } else if (httpAvailable) {
        feedInfoMarkup = `
            <article class="feed-card public-listen-card public-listen-player-card">
                ${titleMarkup}
                <div class="public-listen-toolbar">
                    <label class="public-listen-field">
                        <span>Format</span>
                        <select data-listen-codec>
                            ${HTTP_CODECS.map(([value, label]) => `
                                <option value="${value}" ${codec === value ? 'selected' : ''}>${label}</option>
                            `).join('')}
                        </select>
                    </label>
                    <a class="btn-action public-player-btn" href="${escapeHtml(streamURL)}" data-listen-raw>
                        <i data-lucide="link" width="14" height="14"></i>
                        <span>Raw</span>
                    </a>
                </div>
                <audio class="public-listen-audio" src="${escapeHtml(streamURL)}" controls autoplay playsinline preload="none"></audio>
            </article>
        `;
    }
    listenPanel.innerHTML = `
        ${feedInfoMarkup}
        <div class="public-feed-map-layout">
            <section class="feed-card public-location-card">
                <div class="public-feed-map-heading">
                    <div>
                        <h3>Locations</h3>
                    </div>
                </div>
                <div class="public-location-table-wrap">
                    <table class="public-location-table">
                        <thead data-feed-location-head><tr><th>Location Name</th><th>SAME</th><th>Lat/Lon</th></tr></thead>
                        <tbody data-feed-location-rows><tr><td colspan="3" class="public-location-empty">Loading locations...</td></tr></tbody>
                    </table>
                </div>
            </section>
            <section class="feed-card public-map-card">
                <div class="public-feed-map-heading public-map-toolbar">
                    <div>
                        <h3>Coverage Map</h3>
                    </div>
                    <div class="public-map-toggles">
                        <label class="public-map-toggle">
                            <input type="checkbox" data-feed-border-toggle>
                            <span>Borders</span>
                        </label>
                        <label class="public-map-toggle">
                            <input type="checkbox" data-feed-alert-toggle>
                            <span>Alerts in effect</span>
                        </label>
                    </div>
                </div>
                <p class="status-banner public-map-status" data-feed-map-status>Loading served locations...</p>
                <div class="public-feed-map" data-feed-map aria-label="Map of served CLCs and counties"></div>
            </section>
        </div>
    `;
    const select = listenPanel.querySelector('[data-listen-codec]');
    select?.addEventListener('change', () => {
        const nextCodec = normalizeHTTPCodec(select.value);
        publicFeedGlobalPreferences.mode = 'http';
        publicFeedGlobalPreferences.codec = nextCodec;
        writePublicFeedGlobalPreferences();
        const nextURL = listenPageURL(feedID, false, nextCodec, { includeToken: true });
        window.location.assign(nextURL);
    });
    const webRTCCodec = listenPanel.querySelector('[data-listen-webrtc-codec]');
    webRTCCodec?.addEventListener('change', () => {
        publicFeedGlobalPreferences.mode = 'webrtc';
        publicFeedGlobalPreferences.codec = webRTCCodec.value;
        writePublicFeedGlobalPreferences();
        stopFeed(feedID, { silent: true });
        setPlayerStatus(feedID, 'Codec changed');
    });
    const webRTCPlay = listenPanel.querySelector('[data-feed-play]');
    webRTCPlay?.addEventListener('click', () => {
        publicFeedGlobalPreferences.mode = 'webrtc';
        publicFeedGlobalPreferences.codec = webRTCCodec?.value || DEFAULT_WEBRTC_CODEC;
        writePublicFeedGlobalPreferences();
        startFeedWebRTC(feedID);
    });
    listenPanel.querySelector('[data-feed-stop]')?.addEventListener('click', () => stopFeed(feedID));
    const webRTCAudio = listenPanel.querySelector('[data-feed-audio]');
    webRTCAudio?.addEventListener('play', () => {
        webRTCAudio.dataset.hazeUserPaused = '0';
        const player = feedPlayers.get(feedID);
        if (player?.trackAttached) {
            resumeFeedAudio(feedID, player);
        } else if (webRTCAudio.dataset.hazeWebRTCStarting !== '1') {
            webRTCAudio.dataset.hazeWebRTCStarting = '1';
            startFeedWebRTC(feedID).finally(() => {
                webRTCAudio.dataset.hazeWebRTCStarting = '0';
            });
        }
    });
    webRTCAudio?.addEventListener('pause', () => {
        const player = feedPlayers.get(feedID);
        if (player && !player.stopping) {
            webRTCAudio.dataset.hazeUserPaused = '1';
            setPlayerStatus(feedID, 'Paused');
        }
    });
    initializeFeedMap(feedID);
    window.lucide?.createIcons();
}

function renderPublicState(payload) {
    const summary = payload.summary || {};
    renderSummary(summary);
    if (shouldReconnectPublicFeedsWithToken(summary)) {
        publicFeedTokenReconnectAttempted = true;
        publicClient.includeSessionToken = true;
        publicClient.reconnectNow();
        return;
    }
    if (currentPage === 'alerts') {
        renderPublicAlerts(summary);
        return;
    }
    if (isListenPage) {
        renderListen(summary.feeds || []);
        return;
    }
    if (currentPage !== 'feeds') {
        return;
    }
    if (summary.feeds_access === 'disabled') {
        renderFeeds([]);
        return;
    }
    renderFeeds(summary.feeds || []);
}

function shouldReconnectPublicFeedsWithToken(summary) {
    return currentPage === 'feeds'
        && summary?.feeds_access === 'auth_required'
        && Boolean(session.token)
        && !publicClient.includeSessionToken
        && !publicFeedTokenReconnectAttempted;
}

function connectPublicSocket() {
    publicClient.connect();
}

applyNavState();
applyPageLayout();
window.lucide?.createIcons();
document.querySelectorAll('[data-public-alert-tab]').forEach((button) => {
    button.addEventListener('click', () => {
        activeAlertTab = button.dataset.publicAlertTab || 'accepted';
        document.querySelectorAll('[data-public-alert-tab]').forEach((item) => {
            const active = item === button;
            item.classList.toggle('active', active);
            item.setAttribute('aria-selected', active ? 'true' : 'false');
            item.tabIndex = active ? 0 : -1;
        });
        lastAlertsSignature = '';
        if (summaryState) {
            renderPublicAlerts(summaryState);
        }
    });
});

publicClient.addEventListener('public_state', (event) => renderPublicState(event.detail || {}));
publicClient.addEventListener('open', (event) => {
    if (event.detail?.recovered) {
        if (currentPage === 'feeds') setNotice('Live feed directory recovered.');
        if (currentPage === 'alerts' && alertsNotice) alertsNotice.textContent = 'Alert archive recovered.';
    }
});
publicClient.addEventListener('reconnecting', (event) => {
    const seconds = Math.max(1, Math.round((event.detail?.delay || 1000) / 1000));
    if (currentPage === 'feeds') {
        setNotice(`Live feed directory unavailable. Reconnecting in ${seconds}s...`);
    } else if (currentPage === 'alerts' && alertsNotice) {
        alertsNotice.textContent = `Alert archive unavailable. Reconnecting in ${seconds}s...`;
    }
});
publicClient.addEventListener('recovered', () => {
    if (currentPage !== 'feeds') return;
    reattachActivePlayers();
    for (const [feedId, player] of feedPlayers.entries()) {
        if (player.mode === 'webrtc' && player.trackAttached) {
            setHealthyWebRTCStatus(feedId, player);
        }
    }
});
publicClient.addEventListener('close', (event) => {
    if (!event.detail?.reconnecting) return;
    if (currentPage === 'feeds') {
        setNotice('Live feed directory connection closed. Reconnecting...');
    } else if (currentPage === 'alerts' && alertsNotice) {
        alertsNotice.textContent = 'Alert archive connection closed. Reconnecting...';
    }
});
publicClient.addEventListener('decode_error', () => {
    if (currentPage === 'feeds' && feedNotice) {
        feedNotice.textContent = 'Unable to decode public status update.';
    }
});
publicClient.addEventListener('error', () => {
    if (currentPage === 'feeds') {
        feedNotice.textContent = 'Live feed directory unavailable. Reconnecting...';
    } else if (currentPage === 'alerts' && alertsNotice) {
        alertsNotice.textContent = 'Alert archive unavailable. Reconnecting...';
    }
});

connectPublicSocket();
