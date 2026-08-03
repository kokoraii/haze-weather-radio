import { panelClient } from './lib/ws-client.js';

const byID = (id) => document.getElementById(id);

const cgenView = document.querySelector('.view[data-view="cgen"]');
const statusBanner = byID('cgenStatusBanner');
const pathLabel = byID('cgenPathLabel');
const countMetric = byID('cgenCountMetric');
const modeMetric = byID('cgenModeMetric');
const instanceSelect = byID('cgenInstanceSelect');
const addButton = byID('cgenAddButton');
const saveButton = byID('cgenSaveButton');
const globalEnabled = byID('cgenGlobalEnabled');
const previewStream = byID('cgenPreviewStream');
const previewEmpty = byID('cgenPreviewEmpty');
const metaProgramInput = byID('cgenProgramInputMeta');
const metaPriority = byID('cgenPriorityMeta');
const metaOutput = byID('cgenOutputMeta');
const metaRuntime = byID('cgenRuntimeMeta');
const metaDrift = byID('cgenDriftMeta');
const metaVisual = byID('cgenVisualMeta');
const sceneSelect = byID('cgenSceneSelect');
const sceneXML = byID('cgenSceneXML');
const sceneState = byID('cgenSceneState');
const sceneRefreshButton = byID('cgenSceneRefreshButton');
const sceneNewButton = byID('cgenSceneNewButton');
const sceneSaveButton = byID('cgenSceneSaveButton');
const sceneDeleteButton = byID('cgenSceneDeleteButton');
const outputsBody = byID('cgenOutputsBody');
const addOutputButton = byID('cgenAddOutputButton');
const deinterlaceHint = byID('cgenDeinterlaceHint');
const mappingHint = byID('cgenMappingHint');
const mappingAdvanced = byID('cgenMappingAdvanced');
const cgenTabs = Array.from(document.querySelectorAll('[data-cgen-tab]'));
const cgenTabPanels = Array.from(document.querySelectorAll('[data-cgen-panel]'));

const fields = {
    id: byID('cgenID'),
    name: byID('cgenName'),
    enabled: byID('cgenEnabled'),
    programInputType: byID('cgenProgramInputType'),
    programInput: byID('cgenProgramInput'),
    programInputFormat: byID('cgenProgramInputFormat'),
    hardwareDecoderEnabled: byID('cgenHardwareDecoderEnabled'),
    hardwareDecoder: byID('cgenHardwareDecoder'),
    deinterlaceEnabled: byID('cgenDeinterlaceEnabled'),
    deinterlaceAlgorithm: byID('cgenDeinterlaceAlgorithm'),
    deinterlaceBackend: byID('cgenDeinterlaceBackend'),
    deinterlaceCadence: byID('cgenDeinterlaceCadence'),
    deinterlaceParity: byID('cgenDeinterlaceParity'),
    interlaceEnabled: byID('cgenInterlaceEnabled'),
    interlaceFieldOrder: byID('cgenInterlaceFieldOrder'),
    deviceBackend: byID('cgenDeviceBackend'),
    deviceID: byID('cgenDeviceID'),
    dummyWidth: byID('cgenDummyWidth'),
    dummyHeight: byID('cgenDummyHeight'),
    dummyFPS: byID('cgenDummyFPS'),
    dummyScanMode: byID('cgenDummyScanMode'),
    dummyBackground: byID('cgenDummyBackground'),
    priorityFeed: byID('cgenPriorityFeed'),
    audioSource: byID('cgenAudioSource'),
    audioIdle: byID('cgenAudioIdle'),
    muteStandbyRoutine: byID('cgenMuteStandbyRoutine'),
    captionsPass: byID('cgenCaptionsPass'),
    scte35Pass: byID('cgenScte35Pass'),
    scte104Pass: byID('cgenScte104Pass'),
    audioTopology: byID('cgenAudioTopology'),
    forcedLayout: byID('cgenForcedLayout'),
    idleProgramGain: byID('cgenIdleProgramGain'),
    alertProgramGain: byID('cgenAlertProgramGain'),
    alertGain: byID('cgenAlertGain'),
    audioTransition: byID('cgenAudioTransition'),
    alertScene: byID('cgenAlertScene'),
    pidAssignment: byID('cgenPidAssignment'),
    generatedAlertCues: byID('cgenGeneratedAlertCues'),
    serviceName: byID('cgenServiceName'),
    providerName: byID('cgenProviderName'),
    transportStreamID: byID('cgenTransportStreamID'),
    programNumber: byID('cgenHdProgram'),
    videoPID: byID('cgenHdVideoPID'),
    pmtPID: byID('cgenHdPmtPID'),
    audioTrackID: byID('cgenAudioTrackID'),
    audioPID: byID('cgenAudioPID'),
};

let bound = false;
let cgenEnabled = true;
let cgenRevision = '';
let feeds = [];
let selectedID = '';
let editorDirty = false;
const dirtyFeedIDs = new Set();
let globalDirty = false;
let refreshInFlight = false;
let saveInFlight = false;
let refreshGeneration = 0;
let renderScheduled = false;
let previewStreamFeedID = '';
let previewRetryTimer = 0;
let scenes = [];
let activeScene = null;
let sceneDirty = false;
let cgenCatalog = {
    formats: [],
    video_decoders: [],
    deinterlacers: [],
    devices: [],
    capabilities: {},
};

function setStatus(text, state = 'ok') {
    if (!statusBanner) return;
    statusBanner.textContent = text;
    statusBanner.dataset.state = state;
}

function setText(element, text) {
    if (element && element.textContent !== text) element.textContent = text;
}

function value(key, defaultValue = '') {
    const field = fields[key];
    if (!field) return defaultValue;
    if (field.type === 'checkbox') return field.checked;
    const current = String(field.value ?? '').trim();
    return current || defaultValue;
}

function setValue(key, raw) {
    const field = fields[key];
    if (!field) return;
    if (field.type === 'checkbox') {
        field.checked = raw === true;
        return;
    }
    ensureSelectOption(field, raw);
    field.value = raw ?? '';
}

function option(value, label, { disabled = false, title = '' } = {}) {
    const entry = document.createElement('option');
    entry.value = value;
    entry.textContent = label;
    entry.disabled = disabled;
    if (title) entry.title = title;
    return entry;
}

function ensureSelectOption(select, raw, label = '') {
    if (!select || select.tagName !== 'SELECT') return;
    const value = String(raw ?? '').trim();
    if (!value || Array.from(select.options).some((entry) => entry.value === value)) return;
    select.append(option(value, label || `${value} (configured)`));
}

function sanitizeID(raw) {
    const cleaned = String(raw || '').trim().replace(/[^a-zA-Z0-9_-]+/g, '-').replace(/^-+|-+$/g, '');
    return cleaned || `cgen-${Date.now().toString(36)}`;
}

function escapeHtml(raw) {
    return String(raw ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[character]));
}

function selected() {
    return feeds.find((feed) => feed.id === selectedID) || feeds[0] || null;
}

function setCgenTab(tabID) {
    const activeID = cgenTabs.some((tab) => tab.dataset.cgenTab === tabID)
        ? tabID
        : (cgenTabs[0]?.dataset.cgenTab || '');
    for (const tab of cgenTabs) {
        const active = tab.dataset.cgenTab === activeID;
        tab.classList.toggle('active', active);
        tab.setAttribute('aria-selected', active ? 'true' : 'false');
        tab.tabIndex = active ? 0 : -1;
    }
    for (const panel of cgenTabPanels) panel.hidden = panel.dataset.cgenPanel !== activeID;
}

function bindCgenTabs() {
    for (const tab of cgenTabs) {
        tab.addEventListener('click', () => setCgenTab(tab.dataset.cgenTab || ''));
        tab.addEventListener('keydown', (event) => {
            if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
            event.preventDefault();
            const current = cgenTabs.indexOf(tab);
            let next = current;
            if (event.key === 'Home') next = 0;
            else if (event.key === 'End') next = cgenTabs.length - 1;
            else next = (current + (event.key === 'ArrowRight' ? 1 : -1) + cgenTabs.length) % cgenTabs.length;
            const nextTab = cgenTabs[next];
            setCgenTab(nextTab.dataset.cgenTab || '');
            nextTab.focus();
        });
    }
    setCgenTab(cgenTabs.find((tab) => tab.getAttribute('aria-selected') === 'true')?.dataset.cgenTab);
}

function scheduleRender() {
    if (renderScheduled) return;
    renderScheduled = true;
    window.requestAnimationFrame(() => {
        renderScheduled = false;
        renderMeta();
    });
}

function inputTypeValue() {
    const inputType = String(fields.programInputType?.value || 'uri_or_file').trim().toLowerCase();
    return ['uri_or_file', 'device', 'dummy'].includes(inputType) ? inputType : 'uri_or_file';
}

function updateProgramInputVisibility() {
    const inputType = inputTypeValue();
    document.querySelectorAll('[data-cgen-input-option]').forEach((element) => {
        const supported = String(element.dataset.cgenInputOption || '').split(/\s+/).filter(Boolean);
        element.hidden = !supported.includes(inputType);
    });
    const decoderEnabled = inputType !== 'dummy' && fields.hardwareDecoderEnabled?.checked === true;
    if (fields.hardwareDecoder) fields.hardwareDecoder.disabled = !decoderEnabled || fields.hardwareDecoder.options.length === 0;
    updateDeinterlaceVisibility();
}

function updateDeinterlaceVisibility() {
    const applicable = inputTypeValue() !== 'dummy';
    const enabled = applicable && fields.deinterlaceEnabled?.checked === true;
    document.querySelectorAll('[data-cgen-deinterlace-settings]').forEach((element) => {
        element.hidden = !enabled;
    });
    fields.deinterlaceEnabled?.setAttribute('aria-expanded', enabled ? 'true' : 'false');
    if (enabled) updateDeinterlaceControls();
}

function updateInterlaceVisibility() {
    const enabled = fields.interlaceEnabled?.checked === true;
    document.querySelectorAll('[data-cgen-interlace-settings]').forEach((element) => {
        element.hidden = !enabled;
    });
    fields.interlaceEnabled?.setAttribute('aria-expanded', enabled ? 'true' : 'false');
}

function deinterlacerVariants() {
    return Array.isArray(cgenCatalog.deinterlacers) ? cgenCatalog.deinterlacers : [];
}

function backendLabel(backend) {
    return ({
        auto: 'Best available',
        software: 'Software',
        vaapi: 'VA-API',
        quicksync: 'Intel Quick Sync',
        d3d11: 'Direct3D 11',
        cuda: 'NVIDIA CUDA',
        vulkan: 'Vulkan',
        opengl: 'OpenGL',
    })[backend] || backend;
}

function updateDeinterlaceControls({ chooseAvailable = false } = {}) {
    const variants = deinterlacerVariants();
    if (!fields.deinterlaceAlgorithm || !fields.deinterlaceBackend) return;
    const previousAlgorithm = String(fields.deinterlaceAlgorithm.value || 'yadif');
    const previousBackend = String(fields.deinterlaceBackend.value || 'software');
    if (!variants.length) {
        for (const algorithmOption of fields.deinterlaceAlgorithm.options) {
            algorithmOption.disabled = false;
            algorithmOption.textContent = algorithmOption.value === 'motion_adaptive'
                ? 'Motion adaptive'
                : algorithmOption.value.toUpperCase();
        }
        fields.deinterlaceBackend.replaceChildren(option(
            previousBackend,
            `${backendLabel(previousBackend)} (configured, not verified)`,
        ));
        if (fields.deinterlaceParity) fields.deinterlaceParity.disabled = false;
        if (deinterlaceHint) deinterlaceHint.textContent = 'Runtime capabilities are unavailable. The server will validate this configured choice.';
        return;
    }
    for (const algorithmOption of fields.deinterlaceAlgorithm.options) {
        const candidates = variants.filter((entry) => entry.algorithm === algorithmOption.value);
        const available = candidates.some((entry) => entry.available === true);
        algorithmOption.disabled = !available;
        const base = algorithmOption.value === 'motion_adaptive' ? 'Motion adaptive' : algorithmOption.value.toUpperCase();
        algorithmOption.textContent = candidates.length && !available ? `${base} (unavailable)` : base;
    }
    if (chooseAvailable && fields.deinterlaceAlgorithm.selectedOptions[0]?.disabled) {
        const firstAvailable = Array.from(fields.deinterlaceAlgorithm.options).find((entry) => !entry.disabled);
        if (firstAvailable) fields.deinterlaceAlgorithm.value = firstAvailable.value;
    }
    const algorithm = String(fields.deinterlaceAlgorithm.value || previousAlgorithm);
    const candidates = variants.filter((entry) => entry.algorithm === algorithm);
    const backends = new Map();
    if (candidates.some((entry) => entry.available === true)) {
        backends.set('auto', { available: true, label: 'Best available', reason: '' });
    }
    for (const entry of candidates) {
        const current = backends.get(entry.backend);
        backends.set(entry.backend, {
            available: entry.available === true || current?.available === true,
            label: backendLabel(entry.backend),
            reason: entry.reason || current?.reason || '',
        });
    }
    fields.deinterlaceBackend.replaceChildren();
    for (const [backend, details] of backends) {
        fields.deinterlaceBackend.append(option(
            backend,
            details.available ? details.label : `${details.label} (unavailable)`,
            { disabled: !details.available, title: details.reason },
        ));
    }
    if (!backends.has(previousBackend) && previousBackend) {
        fields.deinterlaceBackend.append(option(previousBackend, `${backendLabel(previousBackend)} (not reported)`, { disabled: true }));
    }
    fields.deinterlaceBackend.value = previousBackend;
    if (chooseAvailable && fields.deinterlaceBackend.selectedOptions[0]?.disabled) {
        const firstAvailable = Array.from(fields.deinterlaceBackend.options).find((entry) => !entry.disabled);
        if (firstAvailable) fields.deinterlaceBackend.value = firstAvailable.value;
    }
    const backend = String(fields.deinterlaceBackend.value || '');
    const selectedVariants = candidates.filter((entry) => backend === 'auto' || entry.backend === backend);
    const availableVariants = selectedVariants.filter((entry) => entry.available === true);
    const parityControl = availableVariants.length > 0 && availableVariants.every((entry) => entry.parity_control === true);
    if (fields.deinterlaceParity) {
        fields.deinterlaceParity.disabled = !parityControl;
        if (!parityControl) fields.deinterlaceParity.value = 'auto';
    }
    if (!deinterlaceHint) return;
    if (!variants.length) {
        deinterlaceHint.textContent = 'The runtime did not provide a deinterlacer capability catalog.';
        return;
    }
    const exact = selectedVariants.find((entry) => entry.backend === backend);
    if (backend === 'auto' && availableVariants.length) {
        deinterlaceHint.textContent = `Runtime choices: ${availableVariants.map((entry) => entry.label).join(', ')}.`;
    } else if (exact?.available) {
        deinterlaceHint.textContent = `${exact.label} is available.`;
    } else {
        deinterlaceHint.textContent = exact?.reason || 'This combination is unavailable on the current runtime.';
    }
}

function preserveNativeAudioAvailable() {
    return cgenCatalog.capabilities?.audio_topologies?.preserve_native_tracks === true;
}

function updateAudioTopologyVisibility() {
    const topology = String(fields.audioTopology?.value || 'force_layout');
    document.querySelectorAll('[data-cgen-audio-option]').forEach((element) => {
        const supported = String(element.dataset.cgenAudioOption || '').split(/\s+/).filter(Boolean);
        element.hidden = !supported.includes(topology);
    });
    const preserve = topology === 'preserve_native_tracks';
    outputsBody?.querySelectorAll('[data-output-field="audio_codec"]').forEach((select) => {
        select.value = preserve ? 'match_input' : (select.value === 'match_input' ? 'aac' : select.value);
        select.disabled = preserve;
    });
}

function applyCatalogCapabilities() {
    const preserveOption = fields.audioTopology?.querySelector('option[value="preserve_native_tracks"]');
    if (!preserveOption) return;
    const available = preserveNativeAudioAvailable();
    preserveOption.disabled = !available;
    preserveOption.textContent = available ? 'Preserve native tracks' : 'Preserve native tracks (unavailable)';
}

function updateProgramMappingVisibility() {
    const mode = String(fields.pidAssignment?.value || 'source');
    document.querySelectorAll('[data-cgen-mapping-option]').forEach((element) => {
        const supported = String(element.dataset.cgenMappingOption || '').split(/\s+/).filter(Boolean);
        element.hidden = !supported.includes(mode);
    });
    if (mappingAdvanced) {
        mappingAdvanced.hidden = mode === 'source';
        if (mode === 'manual') mappingAdvanced.open = true;
    }
    if (mappingHint) {
        mappingHint.textContent = mode === 'source'
            ? 'The input PAT and PMT define the output map.'
            : mode === 'auto'
                ? 'CGEN assigns deterministic, collision-free PIDs.'
                : 'The configured program and PID values are written exactly as validated.';
    }
}

function destinationSelect(current) {
    const select = document.createElement('select');
    select.dataset.outputField = 'destination';
    select.append(
        option('mpeg_ts_udp', 'MPEG-TS / UDP'),
        option('mpeg_ts_srt', 'MPEG-TS / SRT'),
        option('rtp', 'RTP'),
        option('rtmp', 'RTMP'),
        option('file', 'File'),
    );
    select.value = current || 'mpeg_ts_udp';
    return select;
}

function codecSelect(kind, current) {
    const select = document.createElement('select');
    select.dataset.outputField = kind;
    if (kind === 'video_codec') {
        select.append(option('h264', 'H.264'), option('h265', 'H.265'), option('mpeg2', 'MPEG-2'));
    } else {
        select.append(option('aac', 'AAC'), option('ac3', 'AC-3'), option('mp2', 'MP2'), option('match_input', 'Match input'));
    }
    select.value = current || (kind === 'video_codec' ? 'h264' : 'aac');
    return select;
}

function textOutputInput(current, placeholder, field) {
    const input = document.createElement('input');
    input.type = 'text';
    input.autocomplete = 'off';
    input.spellcheck = false;
    input.placeholder = placeholder;
    input.value = Array.isArray(current) ? current.join(', ') : String(current || '');
    input.dataset.outputField = field;
    return input;
}

function numericOutputInput(current, placeholder, field, min, step = '1') {
    const input = document.createElement('input');
    input.type = 'number';
    input.min = String(min);
    input.step = step;
    input.placeholder = placeholder;
    input.value = String(current || '');
    input.dataset.outputField = field;
    input.style.marginTop = '4px';
    return input;
}

function updateOutputRowVisibility(row) {
    const isRTP = row.querySelector('[data-output-field="destination"]')?.value === 'rtp';
    const isVBR = row.querySelector('[data-output-field="rate_control"]')?.value === 'vbr';
    const audioEndpoints = row.querySelector('[data-output-field="audio_urls"]');
    const maxBitrate = row.querySelector('[data-output-field="video_max_bitrate_kbps"]');
    if (audioEndpoints) audioEndpoints.hidden = !isRTP;
    if (maxBitrate) maxBitrate.hidden = !isVBR;
}

function outputRow(rawOutput = {}) {
    const output = rawOutput && typeof rawOutput === 'object' ? rawOutput : {};
    const row = document.createElement('tr');
    row.dataset.outputRow = 'true';
    row._cgenOutputBase = { ...output };

    const enabledCell = document.createElement('td');
    const enabled = document.createElement('input');
    enabled.type = 'checkbox';
    enabled.checked = output.enabled !== false;
    enabled.dataset.outputField = 'enabled';
    enabledCell.append(enabled);

    const idCell = document.createElement('td');
    idCell.append(textOutputInput(output.id || 'output', 'output ID', 'id'));

    const destinationCell = document.createElement('td');
    const destination = destinationSelect(output.destination);
    destinationCell.append(destination);

    const endpointCell = document.createElement('td');
    const endpoint = textOutputInput(output.url || output.video_url, '${OUTPUT_URL}', 'url');
    const audioEndpoints = textOutputInput(output.audio_urls, 'Audio endpoints', 'audio_urls');
    audioEndpoints.style.marginTop = '4px';
    endpointCell.append(endpoint, audioEndpoints);

    const videoCell = document.createElement('td');
    const videoCodec = codecSelect('video_codec', output.video_codec);
    const rateControl = document.createElement('select');
    rateControl.dataset.outputField = 'rate_control';
    rateControl.style.marginTop = '4px';
    rateControl.append(option('cbr', 'CBR'), option('vbr', 'VBR'));
    rateControl.value = output.rate_control || 'cbr';
    videoCell.append(
        videoCodec,
        rateControl,
        numericOutputInput(output.video_bitrate_kbps || '8000', 'target kbps', 'video_bitrate_kbps', 100),
        numericOutputInput(output.video_max_bitrate_kbps || '', 'max kbps', 'video_max_bitrate_kbps', 100),
        numericOutputInput(output.gop_frames || '60', 'GOP frames', 'gop_frames', 1),
    );

    const audioCell = document.createElement('td');
    audioCell.append(
        codecSelect('audio_codec', output.audio_codec),
        numericOutputInput(output.audio_bitrate_kbps || '192', 'audio kbps', 'audio_bitrate_kbps', 32, '8'),
        numericOutputInput(output.sample_rate || '48000', 'sample rate', 'sample_rate', 8000),
    );

    const removeCell = document.createElement('td');
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.className = 'btn-danger';
    remove.textContent = 'Remove';
    remove.addEventListener('click', () => {
        row.remove();
        editorDirty = true;
        if (selectedID) dirtyFeedIDs.add(selectedID);
        scheduleRender();
    });
    removeCell.append(remove);

    destination.addEventListener('change', () => updateOutputRowVisibility(row));
    rateControl.addEventListener('change', () => updateOutputRowVisibility(row));
    row.append(enabledCell, idCell, destinationCell, endpointCell, videoCell, audioCell, removeCell);
    updateOutputRowVisibility(row);
    return row;
}

function renderOutputsEditor(outputs = []) {
    outputsBody?.replaceChildren(...outputs.map((output) => outputRow(output)));
}

function readOutputsEditor() {
    if (!outputsBody) return [];
    const interlaced = value('interlaceEnabled') === true;
    const fieldOrder = value('interlaceFieldOrder', 'tff') === 'bff' ? 'bff' : 'tff';
    return Array.from(outputsBody.querySelectorAll('[data-output-row]')).map((row, index) => {
        const read = (name) => row.querySelector(`[data-output-field="${name}"]`);
        const destination = String(read('destination')?.value || 'mpeg_ts_udp');
        const endpoint = String(read('url')?.value || '').trim();
        return {
            ...(row._cgenOutputBase || {}),
            id: sanitizeID(read('id')?.value || `output-${index + 1}`),
            enabled: read('enabled')?.checked !== false,
            destination,
            url: destination === 'rtp' ? '' : endpoint,
            video_url: destination === 'rtp' ? endpoint : '',
            audio_urls: destination === 'rtp' ? String(read('audio_urls')?.value || '').trim() : '',
            video_codec: String(read('video_codec')?.value || 'h264'),
            rate_control: String(read('rate_control')?.value || 'cbr'),
            video_bitrate_kbps: String(read('video_bitrate_kbps')?.value || '8000'),
            video_max_bitrate_kbps: String(read('video_max_bitrate_kbps')?.value || ''),
            gop_frames: String(read('gop_frames')?.value || '60'),
            interlaced,
            field_order: fieldOrder,
            audio_codec: String(read('audio_codec')?.value || 'aac'),
            audio_bitrate_kbps: String(read('audio_bitrate_kbps')?.value || '192'),
            sample_rate: String(read('sample_rate')?.value || '48000'),
        };
    });
}

function canonicalFeed(raw = {}) {
    const input = raw.program_input && typeof raw.program_input === 'object' ? raw.program_input : {};
    const alert = raw.alert && typeof raw.alert === 'object' ? raw.alert : {};
    const ancillary = raw.ancillary && typeof raw.ancillary === 'object' ? raw.ancillary : {};
    const audio = raw.audio_routing && typeof raw.audio_routing === 'object' ? raw.audio_routing : {};
    const compositor = raw.compositor && typeof raw.compositor === 'object' ? raw.compositor : {};
    const inputType = input.type || 'uri_or_file';
    const dummyInterlaced = input.interlaced ?? false;
    const dummyFieldOrder = input.field_order || 'tff';
    const requestedOutputScan = raw.output_scan && typeof raw.output_scan === 'object' ? raw.output_scan : {};
    const legacyInterlaced = raw.interlaced === true || String(raw.interlaced || '').toLowerCase() === 'true';
    const fallbackInterlaced = requestedOutputScan.interlaced ?? legacyInterlaced;
    const fallbackFieldOrder = requestedOutputScan.field_order || raw.field_order;
    const outputs = Array.isArray(raw.outputs) ? raw.outputs.map((rawOutput) => {
        const output = rawOutput && typeof rawOutput === 'object' ? rawOutput : {};
        const hasScanMode = Object.prototype.hasOwnProperty.call(output, 'interlaced');
        const interlaced = hasScanMode
            ? output.interlaced === true || String(output.interlaced || '').toLowerCase() === 'true'
            : fallbackInterlaced;
        return {
            ...output,
            interlaced,
            field_order: (output.field_order || fallbackFieldOrder) === 'bff' ? 'bff' : 'tff',
        };
    }) : [];
    const scanOutput = outputs.find((output) => output.enabled !== false) || outputs[0] || {};
    return {
        id: String(raw.id || ''),
        original_id: String(raw.original_id || raw.id || ''),
        name: String(raw.name || raw.id || ''),
        enabled: raw.enabled !== false,
        program_input: {
            type: ['uri_or_file', 'device', 'dummy'].includes(inputType) ? inputType : 'uri_or_file',
            url: input.url ?? '',
            format: input.format || 'mpegts',
            hardware_decoder_enabled: input.hardware_decoder_enabled === true,
            hardware_decoder: input.hardware_decoder ?? '',
            deinterlace_enabled: input.deinterlace_enabled !== false,
            deinterlace_algorithm: input.deinterlace_algorithm || 'yadif',
            deinterlace_backend: input.deinterlace_backend || 'software',
            deinterlace_cadence: input.deinterlace_cadence || 'field',
            deinterlace_parity: input.deinterlace_parity || 'auto',
            device_backend: input.device_backend || 'v4l2',
            device_id: input.device_id ?? '',
            width: input.width || '720',
            height: input.height || '480',
            fps: input.fps || '30000/1001',
            interlaced: Boolean(dummyInterlaced),
            field_order: dummyFieldOrder,
            background: input.background || '#000000FF',
        },
        alert: {
            feed_id: alert.feed_id || raw.id || '',
            audio_source: alert.audio_source || 'priority',
            format: alert.format || 'priority-audio',
        },
        ancillary: {
            captions: ancillary.captions || 'drop',
            scte35: ancillary.scte35 || 'drop',
            scte104: ancillary.scte104 || 'drop',
        },
        audio_routing: {
            idle_source: audio.idle_source || 'source',
            alert_mode: audio.alert_mode || 'replace',
            mute_standby_routine: audio.mute_standby_routine !== false,
            topology: audio.topology || 'force_layout',
            force_layout: audio.force_layout || 'stereo',
            idle_program_gain_db: audio.idle_program_gain_db ?? '0',
            alert_program_gain_db: audio.alert_program_gain_db ?? 'muted',
            alert_gain_db: audio.alert_gain_db ?? '0',
            transition_ms: audio.transition_ms || '20',
        },
        compositor: {
            alert_scene_id: compositor.alert_scene_id || 'Standard_Crawl',
            engine: 'scene_v2',
        },
        program_mapping: raw.program_mapping && typeof raw.program_mapping === 'object'
            ? raw.program_mapping
            : defaultProgramMapping(),
        output_scan: {
            interlaced: scanOutput.interlaced ?? fallbackInterlaced,
            field_order: scanOutput.field_order || (fallbackFieldOrder === 'bff' ? 'bff' : 'tff'),
        },
        outputs,
        runtime: raw.runtime && typeof raw.runtime === 'object' ? raw.runtime : {},
    };
}

function defaultProgramMapping() {
    return {
        mode: 'source',
        transport_stream_id: '1',
        programs: [{
            number: '1',
            service_name: 'Haze CGEN',
            provider_name: 'Haze',
            pmt_pid: 'auto',
            video_pid: 'auto',
            audio: [{ track_id: 'primary', pid: 'auto' }],
            scte35: { input: 'drop', generated_alert_cues: true, pid: 'auto' },
        }],
    };
}

function defaultFeed() {
    return canonicalFeed({
        id: 'CFSP-CAP',
        name: 'CFSP/CAP CGEN',
        enabled: true,
        program_input: {
            type: 'uri_or_file',
            url: 'udp://239.0.0.1:9000?fifo_size=2000000&overrun_nonfatal=1&reuse=1&buffer_size=1048576',
            format: 'mpegts',
            hardware_decoder_enabled: false,
            hardware_decoder: '',
            deinterlace_enabled: true,
            deinterlace_algorithm: 'yadif',
            deinterlace_backend: 'software',
            deinterlace_cadence: 'field',
            deinterlace_parity: 'auto',
        },
        alert: { feed_id: 'CFSP-CAP', audio_source: 'priority', format: 'priority-audio' },
        ancillary: { captions: 'drop', scte35: 'drop', scte104: 'drop' },
        audio_routing: {
            idle_source: 'source',
            alert_mode: 'replace',
            mute_standby_routine: true,
            topology: 'force_layout',
            force_layout: 'stereo',
            idle_program_gain_db: '0',
            alert_program_gain_db: 'muted',
            alert_gain_db: '0',
            transition_ms: '20',
        },
        compositor: { alert_scene_id: 'Standard_Crawl', engine: 'scene_v2' },
        program_mapping: defaultProgramMapping(),
        outputs: [{
            id: 'primary',
            enabled: true,
            destination: 'mpeg_ts_udp',
            url: 'udp://239.0.0.2:9001?pkt_size=1316&buffer_size=1048576&reuse=1',
            video_codec: 'h264',
            rate_control: 'cbr',
            video_bitrate_kbps: '8000',
            video_max_bitrate_kbps: '8000',
            gop_frames: '60',
            interlaced: false,
            field_order: 'tff',
            audio_codec: 'aac',
            audio_bitrate_kbps: '192',
            sample_rate: '48000',
        }],
        output_scan: {
            interlaced: false,
            field_order: 'tff',
        },
    });
}

function readEditor() {
    const current = selected() || defaultFeed();
    const id = sanitizeID(value('id'));
    const inputType = inputTypeValue();
    const dummyScan = value('dummyScanMode', 'progressive');
    const alertFeedID = value('priorityFeed', id);
    const mappingMode = value('pidAssignment', 'source');
    const currentPrograms = Array.isArray(current.program_mapping?.programs) ? current.program_mapping.programs : [];
    const currentProgram = currentPrograms[0] || {};
    const currentAudio = Array.isArray(currentProgram.audio) ? currentProgram.audio : [];
    const pid = (key, defaultValue) => mappingMode === 'manual' ? value(key, defaultValue) : 'auto';
    const firstAudio = {
        track_id: value('audioTrackID', currentAudio[0]?.track_id || 'primary'),
        pid: pid('audioPID', '257'),
    };
    const program = {
        ...currentProgram,
        number: value('programNumber', '1'),
        service_name: value('serviceName', 'Haze CGEN'),
        provider_name: value('providerName', 'Haze'),
        pmt_pid: pid('pmtPID', '4096'),
        video_pid: pid('videoPID', '256'),
        audio: [firstAudio, ...currentAudio.slice(1)],
        scte35: {
            input: value('scte35Pass') ? 'pass' : 'drop',
            generated_alert_cues: value('generatedAlertCues'),
            pid: 'auto',
        },
    };
    return {
        id,
        original_id: current.original_id || current.id,
        name: value('name', id),
        enabled: value('enabled'),
        program_input: {
            type: inputType,
            url: value('programInput'),
            format: value('programInputFormat', 'mpegts'),
            hardware_decoder_enabled: value('hardwareDecoderEnabled'),
            hardware_decoder: value('hardwareDecoder'),
            deinterlace_enabled: inputType !== 'dummy' && value('deinterlaceEnabled'),
            deinterlace_algorithm: value('deinterlaceAlgorithm', 'yadif'),
            deinterlace_backend: value('deinterlaceBackend', 'software'),
            deinterlace_cadence: value('deinterlaceCadence', 'field'),
            deinterlace_parity: value('deinterlaceParity', 'auto'),
            device_backend: value('deviceBackend', 'v4l2'),
            device_id: value('deviceID'),
            width: value('dummyWidth', '720'),
            height: value('dummyHeight', '480'),
            fps: value('dummyFPS', '30000/1001'),
            interlaced: dummyScan !== 'progressive',
            field_order: dummyScan === 'interlaced_bff' ? 'bff' : 'tff',
            background: value('dummyBackground', '#000000FF'),
        },
        alert: {
            feed_id: alertFeedID,
            audio_source: value('audioSource', 'priority'),
            format: current.alert?.format || 'priority-audio',
        },
        ancillary: {
            captions: value('captionsPass') ? 'pass' : 'drop',
            scte35: value('scte35Pass') ? 'pass' : 'drop',
            scte104: value('scte104Pass') ? 'pass' : 'drop',
        },
        audio_routing: {
            idle_source: value('audioIdle', 'source'),
            alert_mode: current.audio_routing?.alert_mode || 'replace',
            mute_standby_routine: value('muteStandbyRoutine'),
            topology: value('audioTopology', 'force_layout'),
            force_layout: value('forcedLayout', 'stereo'),
            idle_program_gain_db: value('idleProgramGain', '0'),
            alert_program_gain_db: value('alertProgramGain', 'muted'),
            alert_gain_db: value('alertGain', '0'),
            transition_ms: value('audioTransition', '20'),
        },
        compositor: {
            alert_scene_id: value('alertScene', 'Standard_Crawl'),
            engine: 'scene_v2',
        },
        program_mapping: {
            mode: mappingMode,
            transport_stream_id: value('transportStreamID', '1'),
            programs: [program, ...currentPrograms.slice(1)],
        },
        output_scan: {
            interlaced: value('interlaceEnabled') === true,
            field_order: value('interlaceFieldOrder', 'tff') === 'bff' ? 'bff' : 'tff',
        },
        outputs: readOutputsEditor(),
        runtime: current.runtime || {},
    };
}

function payloadFeed(feed) {
    return {
        id: feed.id,
        original_id: feed.original_id || feed.id,
        name: feed.name,
        enabled: feed.enabled,
        program_input: feed.program_input,
        alert: feed.alert,
        ancillary: feed.ancillary,
        audio_routing: feed.audio_routing,
        compositor: { ...feed.compositor, engine: 'scene_v2' },
        program_mapping: feed.program_mapping,
        outputs: feed.outputs,
    };
}

function validateGain(raw, label) {
    const text = String(raw ?? '').trim().toLowerCase();
    if (text === 'muted') return;
    const number = Number(text);
    if (!Number.isFinite(number) || number < -60 || number > 12) {
        throw new Error(`${label} must be muted or between -60 dB and +12 dB.`);
    }
}

function validatePipelineEditor(feed) {
    if (!feed.alert.feed_id || feed.alert.feed_id === '*') throw new Error('A concrete alert feed ID is required.');
    if (feed.program_input.type === 'uri_or_file' && !String(feed.program_input.url || '').trim()) {
        throw new Error('A program URL, file, or environment reference is required.');
    }
    if (feed.program_input.type === 'device' && !String(feed.program_input.device_id || '').trim()) {
        throw new Error('Select a capture device.');
    }
    if (feed.program_input.deinterlace_enabled) {
        const catalog = deinterlacerVariants();
        const variants = catalog.filter((entry) =>
            entry.algorithm === feed.program_input.deinterlace_algorithm &&
            (feed.program_input.deinterlace_backend === 'auto' || entry.backend === feed.program_input.deinterlace_backend));
        if (catalog.length && !variants.some((entry) => entry.available === true)) {
            throw new Error(variants.find((entry) => entry.reason)?.reason || 'The selected deinterlacer is unavailable.');
        }
    }
    if (!protectedAlertScene(feed.compositor.alert_scene_id)) {
        throw new Error('Program_Passthrough and Standby cannot be alert scenes.');
    }
    if (feed.audio_routing.topology === 'preserve_native_tracks' && !preserveNativeAudioAvailable()) {
        throw new Error('Preserve native tracks is unavailable on the current runtime.');
    }
    validateGain(feed.audio_routing.idle_program_gain_db, 'Idle program gain');
    validateGain(feed.audio_routing.alert_program_gain_db, 'Alert program gain');
    validateGain(feed.audio_routing.alert_gain_db, 'Alert gain');
    if (!feed.outputs.some((output) => output.enabled !== false)) throw new Error('Enable at least one output.');
    if (feed.output_scan?.interlaced && !['tff', 'bff'].includes(feed.output_scan.field_order)) {
        throw new Error('Select a valid output field order.');
    }
    const outputIDs = new Set();
    for (const output of feed.outputs) {
        if (outputIDs.has(output.id)) throw new Error(`Output ID ${output.id} is duplicated.`);
        outputIDs.add(output.id);
        const endpoint = output.destination === 'rtp' ? output.video_url : output.url;
        if (output.enabled !== false && !String(endpoint || '').trim()) throw new Error(`Output ${output.id} requires an endpoint.`);
        if (output.enabled !== false && output.destination === 'rtp' && !String(output.audio_urls || '').trim()) {
            throw new Error(`RTP output ${output.id} requires an audio endpoint.`);
        }
        if (output.enabled !== false && output.destination === 'rtmp' && (output.video_codec !== 'h264' || output.audio_codec !== 'aac')) {
            throw new Error(`RTMP output ${output.id} requires H.264 and AAC.`);
        }
        if (output.enabled !== false && output.rate_control === 'vbr' && Number(output.video_max_bitrate_kbps) < Number(output.video_bitrate_kbps)) {
            throw new Error(`Output ${output.id} max bitrate must be at least its target bitrate.`);
        }
        if (feed.audio_routing.topology === 'preserve_native_tracks' && output.audio_codec !== 'match_input') {
            throw new Error(`Output ${output.id} must match input audio.`);
        }
        if (feed.audio_routing.topology !== 'preserve_native_tracks' && output.audio_codec === 'match_input') {
            throw new Error(`Output ${output.id} requires an encoded audio codec.`);
        }
    }
}

function writeEditor(feed) {
    if (!feed) return;
    const input = feed.program_input;
    const audio = feed.audio_routing;
    const mapping = feed.program_mapping || defaultProgramMapping();
    const program = Array.isArray(mapping.programs) ? mapping.programs[0] || {} : {};
    const primaryAudio = Array.isArray(program.audio) ? program.audio[0] || {} : {};
    setValue('id', feed.id);
    setValue('name', feed.name || feed.id);
    setValue('enabled', feed.enabled !== false);
    setValue('programInputType', input.type || 'uri_or_file');
    setValue('programInput', input.url || '');
    setValue('programInputFormat', input.format || 'mpegts');
    setValue('hardwareDecoderEnabled', input.hardware_decoder_enabled === true);
    setValue('hardwareDecoder', input.hardware_decoder || '');
    setValue('deinterlaceEnabled', input.deinterlace_enabled !== false);
    setValue('deinterlaceAlgorithm', input.deinterlace_algorithm || 'yadif');
    setValue('deinterlaceBackend', input.deinterlace_backend || 'software');
    setValue('deinterlaceCadence', input.deinterlace_cadence || 'field');
    setValue('deinterlaceParity', input.deinterlace_parity || 'auto');
    setValue('deviceBackend', input.device_backend || 'v4l2');
    setValue('deviceID', input.device_id || '');
    setValue('dummyWidth', input.width || '720');
    setValue('dummyHeight', input.height || '480');
    setValue('dummyFPS', input.fps || '30000/1001');
    setValue('dummyScanMode', input.interlaced ? (input.field_order === 'bff' ? 'interlaced_bff' : 'interlaced_tff') : 'progressive');
    setValue('dummyBackground', input.background || '#000000FF');
    setValue('priorityFeed', feed.alert.feed_id || feed.id);
    setValue('audioSource', feed.alert.audio_source || 'priority');
    setValue('audioIdle', audio.idle_source || 'source');
    setValue('muteStandbyRoutine', audio.mute_standby_routine !== false);
    setValue('captionsPass', feed.ancillary.captions === 'pass');
    setValue('scte35Pass', feed.ancillary.scte35 === 'pass');
    setValue('scte104Pass', feed.ancillary.scte104 === 'pass');
    setValue('audioTopology', audio.topology || 'force_layout');
    setValue('forcedLayout', audio.force_layout || 'stereo');
    setValue('idleProgramGain', audio.idle_program_gain_db ?? '0');
    setValue('alertProgramGain', audio.alert_program_gain_db ?? 'muted');
    setValue('alertGain', audio.alert_gain_db ?? '0');
    setValue('audioTransition', audio.transition_ms || '20');
    setValue('alertScene', feed.compositor.alert_scene_id || 'Standard_Crawl');
    setValue('pidAssignment', mapping.mode || 'source');
    setValue('generatedAlertCues', program.scte35?.generated_alert_cues === true);
    setValue('serviceName', program.service_name || 'Haze CGEN');
    setValue('providerName', program.provider_name || 'Haze');
    setValue('transportStreamID', mapping.transport_stream_id || '1');
    setValue('programNumber', program.number || '1');
    setValue('pmtPID', program.pmt_pid === 'auto' ? '4096' : program.pmt_pid || '4096');
    setValue('videoPID', program.video_pid === 'auto' ? '256' : program.video_pid || '256');
    setValue('audioTrackID', primaryAudio.track_id || 'primary');
    setValue('audioPID', primaryAudio.pid === 'auto' ? '257' : primaryAudio.pid || '257');
    setValue('interlaceEnabled', feed.output_scan?.interlaced === true);
    setValue('interlaceFieldOrder', feed.output_scan?.field_order === 'bff' ? 'bff' : 'tff');
    renderOutputsEditor(feed.outputs);
    updateProgramInputVisibility();
    updateDeinterlaceControls();
    updateInterlaceVisibility();
    updateAudioTopologyVisibility();
    updateProgramMappingVisibility();
    scheduleRender();
    editorDirty = false;
}

function upsertEditor() {
    const previousID = selectedID;
    const edited = readEditor();
    const index = feeds.findIndex((feed) => feed.id === selectedID);
    const collision = feeds.some((feed, candidateIndex) =>
        candidateIndex !== index && feed.id.toLowerCase() === edited.id.toLowerCase());
    if (collision) throw new Error(`CGEN pipeline ID ${edited.id} is already in use.`);
    if (index >= 0) feeds[index] = edited;
    else feeds.push(edited);
    selectedID = edited.id;
    if (dirtyFeedIDs.delete(previousID)) dirtyFeedIDs.add(edited.id);
}

function renderInstances() {
    if (!instanceSelect) return;
    instanceSelect.innerHTML = feeds.map((feed) => `<option value="${escapeHtml(feed.id)}">${escapeHtml(feed.name || feed.id)}</option>`).join('');
    if (selectedID && feeds.some((feed) => feed.id === selectedID)) instanceSelect.value = selectedID;
    else if (feeds[0]) {
        selectedID = feeds[0].id;
        instanceSelect.value = selectedID;
    }
    setText(countMetric, String(feeds.length));
    const runtime = selected()?.runtime || {};
    setText(modeMetric, runtime.visual_lifecycle || runtime.gst_state || (selected()?.enabled ? 'enabled' : 'disabled'));
}

function redactEndpointForDisplay(rawValue) {
    const raw = String(rawValue || '').trim();
    if (!raw) return '-';
    if (/\$\{|\$\(|%[A-Za-z_][A-Za-z0-9_]*%/.test(raw)) return '[environment reference]';
    try {
        const parsed = new URL(raw, window.location.origin);
        const explicitScheme = /^[a-z][a-z0-9+.-]*:/i.test(raw);
        parsed.username = '';
        parsed.password = '';
        const hadOptions = parsed.search.length > 1;
        parsed.search = '';
        const summary = `${explicitScheme ? parsed.toString() : parsed.pathname}${hadOptions ? ' [options hidden]' : ''}`;
        return summary.length > 160 ? `${summary.slice(0, 157)}...` : summary;
    } catch {
        return '[configured endpoint]';
    }
}

function programInputSummary(feed) {
    const input = feed.program_input;
    if (input.type === 'device') return `${input.device_backend || 'device'} / ${input.device_id || 'not selected'}`;
    if (input.type === 'dummy') return `dummy ${input.width}x${input.height} ${input.fps}`;
    return redactEndpointForDisplay(input.url);
}

function formatRuntimeAge(raw) {
    const age = Number(raw);
    if (!Number.isFinite(age)) return '';
    return age < 1000 ? ` ${Math.round(age)} ms` : ` ${(age / 1000).toFixed(1)} s`;
}

function formatStreamHealth(label, live, timedOut, age) {
    return `${label} ${live ? 'live' : timedOut ? 'timeout' : 'waiting'}${formatRuntimeAge(age)}`;
}

function formatPipelineDiagnostics(diagnostics) {
    const parts = [];
    const warnings = Number(diagnostics.warning_count);
    const qos = Number(diagnostics.qos_count);
    const latency = Number(diagnostics.latency_recalculation_count);
    if (Number.isFinite(latency)) parts.push(`${latency} latency recalcs`);
    if (Number.isFinite(qos)) parts.push(`${qos} QoS`);
    if (Number.isFinite(warnings)) parts.push(`${warnings} warnings`);
    if (diagnostics.last_warning) parts.push(String(diagnostics.last_warning));
    return parts.join(', ') || '-';
}

function renderMeta() {
    const feed = selected();
    if (!feed) return;
    const runtime = feed.runtime || {};
    const inputHealth = runtime.input_health && typeof runtime.input_health === 'object' ? runtime.input_health : {};
    const diagnostics = runtime.pipeline_diagnostics && typeof runtime.pipeline_diagnostics === 'object' ? runtime.pipeline_diagnostics : {};
    setText(metaProgramInput, programInputSummary(feed));
    setText(metaPriority, `${feed.alert.feed_id || '-'} / ${feed.alert.audio_source || 'priority'}`);
    const enabledOutputs = feed.outputs.filter((output) => output.enabled !== false);
    setText(metaOutput, `${enabledOutputs.length} destination${enabledOutputs.length === 1 ? '' : 's'}`);
    const videoLive = runtime.input_video_connected === true || inputHealth.video_connected === true;
    const audioLive = runtime.input_audio_connected === true || inputHealth.audio_connected === true;
    const video = formatStreamHealth('video', videoLive, inputHealth.video_timed_out === true, inputHealth.last_video_frame_age_ms || inputHealth.last_program_frame_age_ms);
    const audio = formatStreamHealth('audio', audioLive, inputHealth.audio_timed_out === true, inputHealth.last_audio_frame_age_ms);
    setText(metaRuntime, `${video}, ${audio}`);
    setText(metaDrift, formatPipelineDiagnostics(diagnostics));
    setText(metaVisual, runtime.active_scene || runtime.visual_lifecycle || feed.compositor.alert_scene_id || '-');
    setText(modeMetric, runtime.visual_lifecycle || runtime.gst_state || (feed.enabled ? 'enabled' : 'disabled'));
    updatePreviewStream(feed);
}

function updatePreviewStream(feed = selected()) {
    if (!previewStream || !feed?.id) return;
    if (previewStreamFeedID === feed.id && previewStream.src) return;
    previewStreamFeedID = feed.id;
    previewStream.hidden = true;
    if (previewEmpty) {
        previewEmpty.hidden = false;
        previewEmpty.textContent = 'Connecting to live preview...';
    }
    previewStream.src = `/api/v1/cgen/preview?feed=${encodeURIComponent(feed.id)}&t=${Date.now()}`;
}

function catalogID(entry) {
    return String(entry?.id || entry?.value || entry?.persistent_id || entry?.device_id || '').trim();
}

function catalogLabel(entry) {
    return String(entry?.label || entry?.display_name || entry?.name || entry?.id || '').trim();
}

function populateCatalogSelect(select, entries, emptyLabel) {
    if (!select) return;
    const previous = String(select.value || '').trim();
    select.replaceChildren();
    const seen = new Set();
    for (const entry of Array.isArray(entries) ? entries : []) {
        const id = catalogID(entry);
        if (!id || seen.has(id)) continue;
        seen.add(id);
        select.append(option(id, catalogLabel(entry) || id, {
            disabled: entry.available === false,
            title: entry.reason || '',
        }));
    }
    if (previous && seen.has(previous)) select.value = previous;
    if (!select.options.length) {
        select.append(option('', emptyLabel, { disabled: true }));
        select.disabled = true;
    } else {
        select.disabled = false;
    }
}

function populateDeviceSelector() {
    const backend = String(fields.deviceBackend?.value || '').toLowerCase();
    const matches = cgenCatalog.devices.filter((entry) => {
        const entryBackend = String(entry?.backend || entry?.kind || '').toLowerCase();
        return !backend || !entryBackend || entryBackend === backend;
    });
    populateCatalogSelect(fields.deviceID, matches, 'No devices reported');
}

function populateCgenCatalogSelectors() {
    const reportedFormats = cgenCatalog.formats.filter((entry) => ['mpegts', 'rtp', 'srt'].includes(catalogID(entry)));
    populateCatalogSelect(fields.programInputFormat, [
        { id: 'auto', label: 'Auto detect' },
        ...reportedFormats,
        { id: 'file', label: 'File' },
    ], 'No formats reported');
    populateCatalogSelect(fields.hardwareDecoder, cgenCatalog.video_decoders, 'No decoders reported');
    populateDeviceSelector();
    updateDeinterlaceControls();
    updateProgramInputVisibility();
}

async function loadCgenCatalog() {
    const payload = await panelClient.command('cgen.catalog', {}, 15000);
    cgenCatalog = {
        formats: Array.isArray(payload.formats) ? payload.formats : [],
        video_decoders: Array.isArray(payload.video_decoders) ? payload.video_decoders : [],
        deinterlacers: Array.isArray(payload.deinterlacers) ? payload.deinterlacers : [],
        devices: Array.isArray(payload.devices) ? payload.devices : (Array.isArray(payload.input_devices) ? payload.input_devices : []),
        capabilities: payload.capabilities && typeof payload.capabilities === 'object' ? payload.capabilities : {},
    };
    applyCatalogCapabilities();
    populateCgenCatalogSelectors();
}

function protectedAlertScene(id) {
    return id !== 'Program_Passthrough' && id !== 'Standby';
}

function sceneByID(id) {
    return scenes.find((scene) => scene.id === id) || null;
}

function populateAlertSceneOptions() {
    if (!fields.alertScene) return;
    const configured = selected()?.compositor?.alert_scene_id || 'Standard_Crawl';
    const current = String(fields.alertScene.value || configured);
    const available = scenes.filter((scene) => protectedAlertScene(scene.id));
    fields.alertScene.replaceChildren();
    for (const scene of available) fields.alertScene.append(option(scene.id, scene.name || scene.id));
    const safeValue = protectedAlertScene(current) ? current : 'Standard_Crawl';
    ensureSelectOption(fields.alertScene, safeValue);
    fields.alertScene.value = safeValue;
}

function renderSceneCatalog(selectedSceneID = '') {
    if (!sceneSelect) return;
    const selectedValue = selectedSceneID || activeScene?.id || sceneSelect.value;
    sceneSelect.replaceChildren();
    if (!scenes.length) {
        sceneSelect.append(option('', 'No managed scenes found', { disabled: true }));
        sceneSelect.disabled = true;
    } else {
        sceneSelect.disabled = false;
        for (const scene of scenes) {
            const suffix = scene.locked ? ' (locked)' : scene.protected ? ' (protected)' : '';
            sceneSelect.append(option(scene.id, `${scene.name || scene.id}${suffix}`));
        }
        sceneSelect.value = sceneByID(selectedValue)?.id || scenes[0].id;
    }
    populateAlertSceneOptions();
}

function sceneBadge(text) {
    const badge = document.createElement('span');
    badge.className = 'cgen-scene-badge';
    badge.textContent = text;
    return badge;
}

function renderSceneState(message = '') {
    if (!sceneState) return;
    sceneState.replaceChildren();
    if (message) {
        const text = document.createElement('span');
        text.textContent = message;
        sceneState.append(text);
    }
    if (activeScene) {
        sceneState.append(
            sceneBadge(activeScene.id || 'new scene'),
            sceneBadge(activeScene.revision ? `revision ${activeScene.revision.slice(0, 12)}` : 'unsaved'),
        );
        if (activeScene.protected) sceneState.append(sceneBadge('protected'));
        if (activeScene.locked) sceneState.append(sceneBadge('locked'));
    }
    const locked = activeScene?.locked === true;
    if (sceneXML) sceneXML.disabled = locked;
    if (sceneSaveButton) sceneSaveButton.disabled = locked || !activeScene;
    if (sceneDeleteButton) sceneDeleteButton.disabled = !activeScene || activeScene.protected === true;
}

async function loadScene(sceneID) {
    if (!sceneID) {
        activeScene = null;
        if (sceneXML) sceneXML.value = '';
        renderSceneState('Select or create a scene.');
        return;
    }
    const payload = await panelClient.command('cgen.scenes.get', { scene_id: sceneID }, 10000);
    activeScene = payload.scene || null;
    sceneDirty = false;
    if (sceneXML) sceneXML.value = String(activeScene?.xml || '');
    if (sceneSelect && activeScene?.id) sceneSelect.value = activeScene.id;
    renderSceneState('Scene loaded.');
}

async function loadScenes({ selectID = '', announce = false } = {}) {
    const payload = await panelClient.command('cgen.scenes.list', {}, 10000);
    scenes = Array.isArray(payload.scenes) ? payload.scenes : [];
    const nextID = selectID || activeScene?.id || scenes[0]?.id || '';
    renderSceneCatalog(nextID);
    if (nextID && sceneByID(nextID)) await loadScene(nextID);
    else {
        activeScene = null;
        sceneDirty = false;
        if (sceneXML) sceneXML.value = '';
        renderSceneState('No scene selected.');
    }
    if (announce) setStatus(`Scene catalog refreshed: ${scenes.length} documents.`, 'ok');
}

function newSceneXML(id) {
    return `<?xml version="1.0" encoding="UTF-8"?>
<scene schema_version="1" id="${id}" name="${id}">
  <node id="root" name="Root" enabled="true">
    <transform x="0" y="0" width="0" height="0" z_index="0" opacity="1" clip_children="false">
      <anchors left="0" top="0" right="1" bottom="1"/>
    </transform>
    <group/>
  </node>
</scene>
`;
}

function beginNewScene() {
    if (sceneDirty && !window.confirm('Discard unsaved scene XML?')) return;
    const id = `Custom_Scene_${Date.now().toString(36)}`;
    activeScene = { id, name: id, filename: '', revision: '', protected: false, locked: false };
    sceneDirty = true;
    if (sceneSelect) {
        sceneSelect.disabled = false;
        ensureSelectOption(sceneSelect, id, `${id} (unsaved)`);
        sceneSelect.value = id;
    }
    if (sceneXML) {
        sceneXML.disabled = false;
        sceneXML.value = newSceneXML(id);
        sceneXML.focus();
    }
    renderSceneState('New scene is not saved.');
}

async function saveScene() {
    if (!activeScene || activeScene.locked) return;
    const xml = String(sceneXML?.value || '');
    if (!xml.trim()) throw new Error('Scene XML is required.');
    sceneSaveButton.disabled = true;
    try {
        const request = { expected_revision: activeScene.revision || '', xml };
        if (activeScene.revision) {
            request.original_id = activeScene.id;
            request.filename = activeScene.filename;
        }
        const result = await panelClient.command('cgen.scenes.save', request, 12000);
        const savedID = result.changed_scene_id || result.id || activeScene.id;
        sceneDirty = false;
        await loadScenes({ selectID: savedID });
        setStatus(`Scene saved: ${savedID}.`, 'ok');
    } finally {
        renderSceneState();
    }
}

async function deleteScene() {
    if (!activeScene || activeScene.protected) return;
    if (!window.confirm(`Delete scene ${activeScene.name || activeScene.id}?`)) return;
    const deletedID = activeScene.id;
    sceneDeleteButton.disabled = true;
    try {
        await panelClient.command('cgen.scenes.delete', {
            scene_id: activeScene.id,
            expected_revision: activeScene.revision || '',
        }, 10000);
        activeScene = null;
        sceneDirty = false;
        await loadScenes();
        setStatus(`Scene deleted: ${deletedID}.`, 'ok');
    } finally {
        renderSceneState();
    }
}

async function loadCgen() {
    const payload = await panelClient.command('cgen.get', {}, 10000);
    cgenEnabled = payload.enabled !== false;
    cgenRevision = String(payload.revision || payload.config_revision || payload.hash || '');
    dirtyFeedIDs.clear();
    feeds = Array.isArray(payload.feeds) ? payload.feeds.map(canonicalFeed) : [];
    const seededDefault = feeds.length === 0;
    if (seededDefault) {
        const feed = defaultFeed();
        feeds.push(feed);
        dirtyFeedIDs.add(feed.id);
    }
    if (pathLabel) pathLabel.textContent = payload.path || 'managed/configs/cgen.xml';
    if (globalEnabled) globalEnabled.checked = cgenEnabled;
    globalDirty = false;
    renderInstances();
    writeEditor(selected());
    updatePreviewStream(selected());
    setStatus(seededDefault ? 'New CGEN pipeline ready. Save to apply it.' : 'CGEN loaded.', seededDefault ? 'pending' : 'ok');
}

async function refreshRuntime() {
    if (refreshInFlight || saveInFlight) return;
    refreshInFlight = true;
    const generation = refreshGeneration;
    try {
        const payload = await panelClient.command('cgen.get', {}, 10000);
        if (saveInFlight || generation !== refreshGeneration) return;
        const latest = Array.isArray(payload.feeds) ? payload.feeds.map(canonicalFeed) : [];
        const hasFeedEdits = editorDirty || dirtyFeedIDs.size > 0;
        if (!hasFeedEdits && !globalDirty) {
            cgenRevision = String(payload.revision || payload.config_revision || payload.hash || cgenRevision);
        }
        if (!globalDirty) {
            cgenEnabled = payload.enabled !== false;
            if (globalEnabled) globalEnabled.checked = cgenEnabled;
        }
        if (!hasFeedEdits) {
            feeds = latest;
        } else {
            const currentByID = new Map(feeds.map((feed) => [feed.id, feed]));
            const renamedByOriginalID = new Map(feeds
                .filter((feed) => dirtyFeedIDs.has(feed.id) && feed.original_id && feed.original_id !== feed.id)
                .map((feed) => [feed.original_id, feed]));
            const included = new Set();
            const merged = [];
            for (const next of latest) {
                const local = dirtyFeedIDs.has(next.id)
                    ? currentByID.get(next.id)
                    : renamedByOriginalID.get(next.id);
                if (local) {
                    local.runtime = next.runtime;
                    if (!included.has(local.id)) merged.push(local);
                    included.add(local.id);
                } else if (!included.has(next.id)) {
                    merged.push(next);
                    included.add(next.id);
                }
            }
            for (const local of feeds) {
                if (dirtyFeedIDs.has(local.id) && !included.has(local.id)) merged.push(local);
            }
            feeds = merged;
        }
        if (!feeds.some((feed) => feed.id === selectedID)) selectedID = feeds[0]?.id || '';
        renderInstances();
        if (!editorDirty) writeEditor(selected());
        else scheduleRender();
    } finally {
        refreshInFlight = false;
    }
}

async function saveCgen() {
    if (saveInFlight) return;
    saveInFlight = true;
    refreshGeneration += 1;
    if (saveButton) saveButton.disabled = true;
    if (cgenView) {
        cgenView.inert = true;
        cgenView.setAttribute('aria-busy', 'true');
    }
    setStatus('Saving CGEN...', 'pending');
    try {
        const edited = readEditor();
        validatePipelineEditor(edited);
        upsertEditor();
        const payload = await panelClient.command('cgen.save', {
            enabled: globalEnabled?.checked === true,
            feeds: feeds.map(payloadFeed),
            expected_revision: cgenRevision,
        }, 12000);
        cgenEnabled = payload.enabled !== false;
        cgenRevision = String(payload.revision || payload.config_revision || payload.hash || cgenRevision);
        feeds = Array.isArray(payload.feeds) ? payload.feeds.map(canonicalFeed) : feeds;
        dirtyFeedIDs.clear();
        globalDirty = false;
        if (globalEnabled) globalEnabled.checked = cgenEnabled;
        renderInstances();
        writeEditor(selected());
        setStatus('CGEN saved.', 'ok');
    } finally {
        saveInFlight = false;
        if (saveButton) saveButton.disabled = false;
        if (cgenView) {
            cgenView.inert = false;
            cgenView.removeAttribute('aria-busy');
        }
    }
}

function addInstance() {
    upsertEditor();
    const next = defaultFeed();
    let suffix = feeds.length + 1;
    while (feeds.some((feed) => feed.id.toLowerCase() === `cgen-${suffix}`)) suffix += 1;
    next.id = `cgen-${suffix}`;
    next.original_id = next.id;
    next.name = `CGEN ${feeds.length + 1}`;
    next.alert.feed_id = next.id;
    feeds.push(next);
    selectedID = next.id;
    dirtyFeedIDs.add(next.id);
    renderInstances();
    writeEditor(next);
    setStatus('New CGEN pipeline ready.', 'pending');
}

export function initCgenView() {
    if (bound) return;
    bound = true;
    bindCgenTabs();
    renderSceneState('Scene catalog not loaded.');
    addButton?.addEventListener('click', () => {
        try {
            addInstance();
        } catch (error) {
            setStatus(error.message || 'Unable to add a CGEN pipeline.', 'err');
        }
    });
    globalEnabled?.addEventListener('change', () => {
        globalDirty = true;
    });
    saveButton?.addEventListener('click', () => saveCgen().catch((error) => setStatus(error.message || 'Unable to save CGEN.', 'err')));
    instanceSelect?.addEventListener('change', () => {
        const nextID = instanceSelect.value;
        try {
            upsertEditor();
        } catch (error) {
            instanceSelect.value = selectedID;
            setStatus(error.message || 'Unable to switch CGEN pipelines.', 'err');
            return;
        }
        selectedID = nextID;
        renderInstances();
        writeEditor(selected());
        updatePreviewStream(selected());
    });
    for (const field of Object.values(fields)) {
        field?.addEventListener('input', () => {
            editorDirty = true;
            if (selectedID) dirtyFeedIDs.add(selectedID);
            if (field === fields.programInputType || field === fields.hardwareDecoderEnabled) updateProgramInputVisibility();
            if (field === fields.deinterlaceEnabled) updateDeinterlaceVisibility();
            if (field === fields.interlaceEnabled) updateInterlaceVisibility();
            if (field === fields.deinterlaceAlgorithm) updateDeinterlaceControls({ chooseAvailable: true });
            if (field === fields.deinterlaceBackend) updateDeinterlaceControls();
            if (field === fields.audioTopology) updateAudioTopologyVisibility();
            if (field === fields.pidAssignment) updateProgramMappingVisibility();
            if (field === fields.deviceBackend) populateDeviceSelector();
            scheduleRender();
        });
        field?.addEventListener('change', () => {
            editorDirty = true;
            if (selectedID) dirtyFeedIDs.add(selectedID);
            if (field === fields.programInputType || field === fields.hardwareDecoderEnabled) updateProgramInputVisibility();
            if (field === fields.deinterlaceEnabled) updateDeinterlaceVisibility();
            if (field === fields.deinterlaceAlgorithm) updateDeinterlaceControls({ chooseAvailable: true });
            if (field === fields.deinterlaceBackend) updateDeinterlaceControls();
            if (field === fields.audioTopology) updateAudioTopologyVisibility();
            if (field === fields.pidAssignment) updateProgramMappingVisibility();
            if (field === fields.deviceBackend) populateDeviceSelector();
            scheduleRender();
        });
    }
    outputsBody?.addEventListener('input', () => {
        editorDirty = true;
        if (selectedID) dirtyFeedIDs.add(selectedID);
        scheduleRender();
    });
    outputsBody?.addEventListener('change', () => {
        editorDirty = true;
        if (selectedID) dirtyFeedIDs.add(selectedID);
        scheduleRender();
    });
    addOutputButton?.addEventListener('click', () => {
        const index = outputsBody?.querySelectorAll('[data-output-row]').length || 0;
        outputsBody?.append(outputRow({
            id: `output-${index + 1}`,
            enabled: true,
            destination: 'mpeg_ts_udp',
            video_codec: 'h264',
            rate_control: 'cbr',
            video_bitrate_kbps: '8000',
            gop_frames: '60',
            interlaced: fields.interlaceEnabled?.checked === true,
            field_order: fields.interlaceFieldOrder?.value === 'bff' ? 'bff' : 'tff',
            audio_codec: fields.audioTopology?.value === 'preserve_native_tracks' ? 'match_input' : 'aac',
            audio_bitrate_kbps: '192',
            sample_rate: '48000',
        }));
        updateAudioTopologyVisibility();
        editorDirty = true;
        if (selectedID) dirtyFeedIDs.add(selectedID);
        scheduleRender();
    });
    sceneXML?.addEventListener('input', () => {
        sceneDirty = true;
        renderSceneState('Unsaved scene changes.');
    });
    sceneSelect?.addEventListener('change', () => {
        const nextID = sceneSelect.value;
        if (sceneDirty && !window.confirm('Discard unsaved scene XML?')) {
            sceneSelect.value = activeScene?.id || '';
            return;
        }
        loadScene(nextID).catch((error) => setStatus(error.message || 'Unable to load scene.', 'err'));
    });
    sceneRefreshButton?.addEventListener('click', () => {
        if (sceneDirty && !window.confirm('Discard unsaved scene XML and refresh?')) return;
        loadScenes({ announce: true }).catch((error) => setStatus(error.message || 'Unable to refresh scenes.', 'err'));
    });
    sceneNewButton?.addEventListener('click', beginNewScene);
    sceneSaveButton?.addEventListener('click', () => saveScene().catch((error) => setStatus(error.message || 'Unable to save scene.', 'err')));
    sceneDeleteButton?.addEventListener('click', () => deleteScene().catch((error) => setStatus(error.message || 'Unable to delete scene.', 'err')));
    previewStream?.addEventListener('load', () => {
        previewStream.hidden = false;
        if (previewEmpty) previewEmpty.hidden = true;
    });
    previewStream?.addEventListener('error', () => {
        previewStream.hidden = true;
        if (previewEmpty) {
            previewEmpty.hidden = false;
            previewEmpty.textContent = 'Live preview is unavailable.';
        }
        window.clearTimeout(previewRetryTimer);
        previewRetryTimer = window.setTimeout(() => {
            previewStreamFeedID = '';
            updatePreviewStream(selected());
        }, 2500);
    });
    const catalogRequest = loadCgenCatalog();
    const configRequest = catalogRequest.catch(() => undefined).then(() => loadCgen());
    Promise.allSettled([catalogRequest, configRequest, loadScenes()]).then((results) => {
        const catalogError = results[0].status === 'rejected' ? results[0].reason : null;
        const configError = results[1].status === 'rejected' ? results[1].reason : null;
        const sceneError = results[2].status === 'rejected' ? results[2].reason : null;
        if (configError) setStatus(configError.message || 'Unable to load CGEN.', 'err');
        else if (catalogError) setStatus(catalogError.message || 'CGEN capability catalog is unavailable.', 'err');
        else if (sceneError) setStatus(sceneError.message || 'CGEN scenes are unavailable.', 'err');
        if (!configError) {
            window.setInterval(() => {
                refreshRuntime().catch(() => scheduleRender());
            }, 1500);
        }
    });
}
