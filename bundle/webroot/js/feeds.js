import { panelClient } from './lib/ws-client.js';

const statusBanner = document.getElementById('feedsStatusBanner');
const tableBody = document.getElementById('feedTableBody');
const countMetric = document.getElementById('feedsCountMetric');
const selectedMetric = document.getElementById('feedsSelectedMetric');
const pathLabel = document.getElementById('feedsPathLabel');
const selectedLabel = document.getElementById('feedSelectedLabel');
const addButton = document.getElementById('feedAddButton');
const deleteButton = document.getElementById('feedDeleteButton');
const saveButton = document.getElementById('feedSaveButton');

const fields = {
    id: document.getElementById('feedID'),
    enabled: document.getElementById('feedEnabled'),
    timezone: document.getElementById('feedTimezone'),
    routine: document.getElementById('feedRoutine'),
    same: document.getElementById('feedSame'),
    sameOriginator: document.getElementById('feedSameOriginator'),
    sameAttentionTone: document.getElementById('feedSameAttentionTone'),
    capCPEnabled: document.getElementById('feedCapCPEnabled'),
    nwsCAPEnabled: document.getElementById('feedNWSCAPEnabled'),
    siteName: document.getElementById('feedSiteName'),
    callsign: document.getElementById('feedCallsign'),
    relationship: document.getElementById('feedRelationship'),
    frequency: document.getElementById('feedFrequency'),
    languages: document.getElementById('feedLanguages'),
    descriptionText: document.getElementById('feedDescriptionText'),
    descriptionSuffix: document.getElementById('feedDescriptionSuffix'),
    coverageRegions: document.getElementById('feedCoverageRegions'),
    observationLocations: document.getElementById('feedObservationLocations'),
    aviationReportLocations: document.getElementById('feedAviationReportLocations'),
    airQualityLocations: document.getElementById('feedAirQualityLocations'),
    climateLocations: document.getElementById('feedClimateLocations'),
    hydrometricLocations: document.getElementById('feedHydrometricLocations'),
    hydrometricUpstreamLocations: document.getElementById('feedHydrometricUpstreamLocations'),
    hydrometricDownstreamLocations: document.getElementById('feedHydrometricDownstreamLocations'),
    marineForecastLocations: document.getElementById('feedMarineForecastLocations'),
    marineForecastSubregions: document.getElementById('feedMarineForecastSubregions'),
    marineConditionLocations: document.getElementById('feedMarineConditionLocations'),
    capCPUseLocations: document.getElementById('feedCapCPUseLocations'),
    nwsCAPUseLocations: document.getElementById('feedNWSCAPUseLocations'),
    capCPAllowlist: document.getElementById('feedCapCPAllowlist'),
    capCPBlocklist: document.getElementById('feedCapCPBlocklist'),
    nwsCAPAllowlist: document.getElementById('feedNWSCAPAllowlist'),
    nwsCAPBlocklist: document.getElementById('feedNWSCAPBlocklist'),
};

let bound = false;
let feeds = [];
let playlist = { feeds: [] };
let selectedID = '';

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[char]));
}

function setStatus(text, state = 'ok') {
    statusBanner.textContent = text;
    statusBanner.dataset.state = state;
}

function sanitizeID(value) {
    return String(value || '').trim().replace(/[^a-zA-Z0-9_.:-]+/g, '-').replace(/^-+|-+$/g, '') || `feed-${Date.now().toString(36)}`;
}

function selected() {
    return feeds.find((feed) => feed.id === selectedID) || feeds[0] || null;
}

function runtimeFor(id) {
    return (playlist.feeds || []).find((feed) => (feed.feed_id || feed.id) === id) || {};
}

function runtimeMode(feed) {
    return String(feed?.runtime?.mode || runtimeFor(feed?.id || '').mode || 'unknown');
}

function queueDepth(feed) {
    const runtime = feed?.runtime || runtimeFor(feed?.id || '');
    return Array.isArray(runtime.queue) ? runtime.queue.length : Number(runtime.queue_depth || 0);
}

function modeState(mode) {
    const value = String(mode || '').toLowerCase();
    if (['running', 'playing', 'active', 'live'].includes(value)) return 'running';
    if (['paused', 'pending', 'queued', 'waiting'].includes(value)) return 'paused';
    if (['stopped', 'disabled', 'error', 'failed'].includes(value)) return 'stopped';
    return 'unknown';
}

function readEditor() {
    return {
        id: sanitizeID(fields.id.value),
        enabled: fields.enabled.checked,
        timezone: fields.timezone.value.trim() || 'Local',
        routine: fields.routine.checked,
        same: fields.same.checked,
        same_originator: fields.sameOriginator.value || 'EAS',
        same_attention_tone: fields.sameAttentionTone.value.trim(),
        cap_cp_enabled: fields.capCPEnabled.checked,
        nws_cap_enabled: fields.nwsCAPEnabled.checked,
        site_name: fields.siteName.value.trim(),
        callsign: fields.callsign.value.trim(),
        relationship: fields.relationship.value.trim() || 'primary',
        frequency_mhz: fields.frequency.value.trim(),
        languages: fields.languages.value,
        description_lang: 'en-CA',
        description_text: fields.descriptionText.value.trim(),
        description_suffix: fields.descriptionSuffix.value.trim(),
        coverage_regions: fields.coverageRegions.value,
        observation_locations: fields.observationLocations.value,
        aviation_report_locations: fields.aviationReportLocations.value,
        air_quality_locations: fields.airQualityLocations.value,
        climate_locations: fields.climateLocations.value,
        hydrometric_locations: fields.hydrometricLocations.value,
        hydrometric_upstream_locations: fields.hydrometricUpstreamLocations.value,
        hydrometric_downstream_locations: fields.hydrometricDownstreamLocations.value,
        marine_forecast_locations: fields.marineForecastLocations.value,
        marine_forecast_subregions: fields.marineForecastSubregions.value,
        marine_condition_locations: fields.marineConditionLocations.value,
        cap_cp_use_feed_locations: fields.capCPUseLocations.value === 'true',
        nws_cap_use_feed_locations: fields.nwsCAPUseLocations.value === 'true',
        cap_cp_allowlist: fields.capCPAllowlist.value,
        cap_cp_blocklist: fields.capCPBlocklist.value,
        nws_cap_allowlist: fields.nwsCAPAllowlist.value,
        nws_cap_blocklist: fields.nwsCAPBlocklist.value,
    };
}

function updateSelectedFromEditor() {
    const current = selected();
    if (!current) return;
    const next = readEditor();
    const oldID = current.id;
    Object.assign(current, next);
    if (next.id !== oldID) selectedID = next.id;
}

function setField(key, value) {
    const field = fields[key];
    if (!field) return;
    if (field.type === 'checkbox') field.checked = Boolean(value);
    else field.value = value ?? '';
}

function writeEditor(feed) {
    const empty = !feed;
    Object.values(fields).forEach((field) => { if (field) field.disabled = empty; });
    deleteButton.disabled = empty;
    saveButton.disabled = empty;
    if (!feed) {
        Object.values(fields).forEach((field) => {
            if (!field) return;
            if (field.type === 'checkbox') field.checked = false;
            else field.value = '';
        });
        selectedLabel.textContent = 'No feed selected';
        selectedMetric.textContent = 'none';
        return;
    }
    setField('id', feed.id);
    setField('enabled', feed.enabled !== false);
    setField('timezone', feed.timezone || 'Local');
    setField('routine', feed.routine !== false);
    setField('same', feed.same !== false);
    setField('sameOriginator', feed.same_originator || 'EAS');
    setField('sameAttentionTone', feed.same_attention_tone || '');
    setField('capCPEnabled', feed.cap_cp_enabled !== false);
    setField('nwsCAPEnabled', Boolean(feed.nws_cap_enabled));
    setField('siteName', feed.site_name || feed.id);
    setField('callsign', feed.callsign || '');
    setField('relationship', feed.relationship || 'primary');
    setField('frequency', feed.frequency_mhz || '');
    setField('languages', feed.languages || 'en-CA:0');
    setField('descriptionText', feed.description_text || '');
    setField('descriptionSuffix', feed.description_suffix || '');
    setField('coverageRegions', feed.coverage_regions || '');
    setField('observationLocations', feed.observation_locations || '');
    setField('aviationReportLocations', feed.aviation_report_locations || '');
    setField('airQualityLocations', feed.air_quality_locations || '');
    setField('climateLocations', feed.climate_locations || '');
    setField('hydrometricLocations', feed.hydrometric_locations || '');
    setField('hydrometricUpstreamLocations', feed.hydrometric_upstream_locations || '');
    setField('hydrometricDownstreamLocations', feed.hydrometric_downstream_locations || '');
    setField('marineForecastLocations', feed.marine_forecast_locations || '');
    setField('marineForecastSubregions', feed.marine_forecast_subregions || '');
    setField('marineConditionLocations', feed.marine_condition_locations || '');
    setField('capCPUseLocations', String(feed.cap_cp_use_feed_locations !== false));
    setField('nwsCAPUseLocations', String(feed.nws_cap_use_feed_locations !== false));
    setField('capCPAllowlist', feed.cap_cp_allowlist || '');
    setField('capCPBlocklist', feed.cap_cp_blocklist || '');
    setField('nwsCAPAllowlist', feed.nws_cap_allowlist || '');
    setField('nwsCAPBlocklist', feed.nws_cap_blocklist || '');
    selectedLabel.textContent = feed.id;
    selectedMetric.textContent = runtimeMode(feed);
}

function renderTable() {
    countMetric.textContent = String(feeds.length);
    if (!feeds.length) {
        tableBody.innerHTML = '<tr><td colspan="4" class="panel-empty-cell">No feeds configured.</td></tr>';
        writeEditor(null);
        return;
    }
    if (!selectedID || !feeds.some((feed) => feed.id === selectedID)) selectedID = feeds[0].id;
    tableBody.innerHTML = feeds.map((feed) => {
        const mode = runtimeMode(feed);
        const pauseAction = mode === 'paused' ? 'unpause' : 'pause';
        const stopAction = mode === 'stopped' ? 'restart' : 'stop';
        const active = feed.id === selectedID;
        const enabledState = feed.enabled !== false ? 'enabled' : 'disabled';
        const depth = queueDepth(feed);
        return `
            <tr class="feed-row ${active ? 'active' : ''}" data-feed-id="${escapeHtml(feed.id)}" aria-selected="${active ? 'true' : 'false'}" tabindex="0">
                <td>
                    <div class="feed-row-title">
                        <strong>${escapeHtml(feed.site_name || feed.id)}</strong>
                        <span><code>${escapeHtml(feed.id)}</code> <span class="table-pill" data-state="${enabledState}">${enabledState}</span></span>
                    </div>
                </td>
                <td><span class="table-pill" data-state="${escapeHtml(modeState(mode))}" title="${escapeHtml(mode)}">${escapeHtml(mode)}</span></td>
                <td><span class="table-queue" data-active="${depth > 0 ? 'true' : 'false'}">${escapeHtml(depth)}</span></td>
                <td>
                    <div class="feed-row-actions table-row-actions">
                        <button class="btn-action" type="button" data-feed-control="${pauseAction}">${pauseAction === 'pause' ? 'Pause' : 'Unpause'}</button>
                        <button class="${stopAction === 'stop' ? 'btn-danger' : 'btn-action'}" type="button" data-feed-control="${stopAction}">${stopAction === 'stop' ? 'Stop' : 'Restart'}</button>
                    </div>
                </td>
            </tr>
        `;
    }).join('');
    writeEditor(selected());
}

async function loadFeeds() {
    setStatus('Loading feeds...', 'pending');
    const payload = await panelClient.command('feeds.get', {}, 10000);
    feeds = Array.isArray(payload.feeds) ? payload.feeds : [];
    playlist = payload.playlist || playlist;
    pathLabel.textContent = payload.configured || payload.path || 'managed/configs/feeds.xml';
    renderTable();
    setStatus(`Loaded ${feeds.length} feed${feeds.length === 1 ? '' : 's'}.`, 'ok');
}

async function saveFeeds() {
    updateSelectedFromEditor();
    setStatus('Saving feeds.xml...', 'pending');
    const payload = await panelClient.command('feeds.save', { feeds }, 15000);
    feeds = Array.isArray(payload.feeds) ? payload.feeds : feeds;
    playlist = payload.playlist || playlist;
    pathLabel.textContent = payload.configured || payload.path || pathLabel.textContent;
    renderTable();
    setStatus('Saved feeds.xml. Restart affected services for structural changes.', 'ok');
}

async function controlFeed(id, action) {
    setStatus(`${action} sent to ${id}...`, 'pending');
    const result = await panelClient.command('feeds.control', { feed_id: id, action }, 10000);
    if (result?.playlist) playlist = result.playlist;
    await loadFeeds();
    setStatus(`${action} accepted for ${id}.`, result?.settled ? 'ok' : 'pending');
}

function addFeed() {
    updateSelectedFromEditor();
    let n = feeds.length + 1;
    let id = `feed-${String(n).padStart(2, '0')}`;
    while (feeds.some((feed) => feed.id === id)) {
        n += 1;
        id = `feed-${String(n).padStart(2, '0')}`;
    }
    feeds.push({
        id,
        enabled: true,
        timezone: 'Local',
        routine: true,
        same: true,
        same_originator: 'EAS',
        cap_cp_enabled: true,
        cap_cp_use_feed_locations: true,
        nws_cap_enabled: false,
        nws_cap_use_feed_locations: true,
        languages: 'en-CA:0',
        site_name: id,
        relationship: 'primary',
    });
    selectedID = id;
    renderTable();
    fields.id.focus();
    setStatus('New feed created. Save XML to persist.', 'pending');
}

function deleteFeed() {
    const current = selected();
    if (!current) return;
    feeds = feeds.filter((feed) => feed.id !== current.id);
    selectedID = feeds[0]?.id || '';
    renderTable();
    setStatus(`Removed ${current.id}. Save XML to persist.`, 'pending');
}

function bind() {
    if (bound) return;
    bound = true;
    tableBody.addEventListener('click', (event) => {
        const control = event.target.closest('[data-feed-control]');
        if (control) {
            const row = control.closest('[data-feed-id]');
            if (row) controlFeed(row.dataset.feedId, control.dataset.feedControl).catch((err) => setStatus(err.message, 'err'));
            return;
        }
        const row = event.target.closest('[data-feed-id]');
        if (!row) return;
        updateSelectedFromEditor();
        selectedID = row.dataset.feedId;
        renderTable();
    });
    tableBody.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        if (event.target.closest('button')) return;
        const row = event.target.closest('[data-feed-id]');
        if (!row) return;
        event.preventDefault();
        updateSelectedFromEditor();
        selectedID = row.dataset.feedId;
        renderTable();
    });
    Object.values(fields).forEach((field) => {
        if (!field) return;
        field.addEventListener('input', () => {
            updateSelectedFromEditor();
            selectedLabel.textContent = selected()?.id || 'No feed selected';
            setStatus('Unsaved feed changes.', 'pending');
        });
        field.addEventListener('change', () => {
            updateSelectedFromEditor();
            renderTable();
            setStatus('Unsaved feed changes.', 'pending');
        });
    });
    addButton.addEventListener('click', addFeed);
    deleteButton.addEventListener('click', deleteFeed);
    saveButton.addEventListener('click', () => saveFeeds().catch((err) => setStatus(err.message, 'err')));
    window.addEventListener('haze:admin-state', (event) => {
        if (event.detail?.playlist?.feeds) {
            playlist = event.detail.playlist;
            renderTable();
        }
    });
}

export function initFeedsView() {
    bind();
    loadFeeds().catch((err) => setStatus(err.message, 'err'));
}
