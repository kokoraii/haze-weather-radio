import { panelClient } from './lib/ws-client.js';

const statusBanner = document.getElementById('cgenStatusBanner');
const pathLabel = document.getElementById('cgenPathLabel');
const countMetric = document.getElementById('cgenCountMetric');
const modeMetric = document.getElementById('cgenModeMetric');
const instanceSelect = document.getElementById('cgenInstanceSelect');
const addButton = document.getElementById('cgenAddButton');
const saveButton = document.getElementById('cgenSaveButton');
const globalEnabled = document.getElementById('cgenGlobalEnabled');
const preview = document.getElementById('cgenPreview');
const previewStream = document.getElementById('cgenPreviewStream');
const metaProgramInput = document.getElementById('cgenProgramInputMeta');
const metaPriority = document.getElementById('cgenPriorityMeta');
const metaOutput = document.getElementById('cgenOutputMeta');
const metaRuntime = document.getElementById('cgenRuntimeMeta');
const metaDrift = document.getElementById('cgenDriftMeta');
const metaVisual = document.getElementById('cgenVisualMeta');
const sunnyField = document.getElementById('cgenSunnyField');
const sunnyButton = document.getElementById('cgenSunnyButton');
const refreshFontsButton = document.getElementById('cgenRefreshFontsButton');
const fontPreview = document.getElementById('cgenFontPreview');
const fontPicker = document.getElementById('cgenFontPicker');
const fontPickerLabel = document.getElementById('cgenFontPickerLabel');
const fontMenu = document.getElementById('cgenFontMenu');
const sceneSelect = document.getElementById('cgenSceneSelect');
const sceneXML = document.getElementById('cgenSceneXML');
const sceneState = document.getElementById('cgenSceneState');
const sceneRefreshButton = document.getElementById('cgenSceneRefreshButton');
const sceneNewButton = document.getElementById('cgenSceneNewButton');
const sceneSaveButton = document.getElementById('cgenSceneSaveButton');
const sceneDeleteButton = document.getElementById('cgenSceneDeleteButton');
const outputsBody = document.getElementById('cgenOutputsBody');
const addOutputButton = document.getElementById('cgenAddOutputButton');
const deinterlaceHint = document.getElementById('cgenDeinterlaceHint');
const cgenTabs = Array.from(document.querySelectorAll('[data-cgen-tab]'));
const cgenTabPanels = Array.from(document.querySelectorAll('[data-cgen-panel]'));

const fields = {
    id: document.getElementById('cgenID'),
    name: document.getElementById('cgenName'),
    enabled: document.getElementById('cgenEnabled'),
    mode: document.getElementById('cgenMode'),
    programInputType: document.getElementById('cgenProgramInputType'),
    programInput: document.getElementById('cgenProgramInput'),
    programInputFormat: document.getElementById('cgenProgramInputFormat'),
    hardwareDecoderEnabled: document.getElementById('cgenHardwareDecoderEnabled'),
    hardwareDecoder: document.getElementById('cgenHardwareDecoder'),
    deinterlaceAlgorithm: document.getElementById('cgenDeinterlaceAlgorithm'),
    deinterlaceBackend: document.getElementById('cgenDeinterlaceBackend'),
    deinterlaceCadence: document.getElementById('cgenDeinterlaceCadence'),
    deinterlaceParity: document.getElementById('cgenDeinterlaceParity'),
    deviceBackend: document.getElementById('cgenDeviceBackend'),
    deviceID: document.getElementById('cgenDeviceID'),
    dummyWidth: document.getElementById('cgenDummyWidth'),
    dummyHeight: document.getElementById('cgenDummyHeight'),
    dummyFPS: document.getElementById('cgenDummyFPS'),
    dummyScanMode: document.getElementById('cgenDummyScanMode'),
    dummyBackground: document.getElementById('cgenDummyBackground'),
    priorityFeed: document.getElementById('cgenPriorityFeed'),
    audioSource: document.getElementById('cgenAudioSource'),
    audioIdle: document.getElementById('cgenAudioIdle'),
    muteStandbyRoutine: document.getElementById('cgenMuteStandbyRoutine'),
    captionsPass: document.getElementById('cgenCaptionsPass'),
    scte35Pass: document.getElementById('cgenScte35Pass'),
    scte104Pass: document.getElementById('cgenScte104Pass'),
    audioTopology: document.getElementById('cgenAudioTopology'),
    forcedLayout: document.getElementById('cgenForcedLayout'),
    idleProgramGain: document.getElementById('cgenIdleProgramGain'),
    alertProgramGain: document.getElementById('cgenAlertProgramGain'),
    alertGain: document.getElementById('cgenAlertGain'),
    audioTransition: document.getElementById('cgenAudioTransition'),
    alertScene: document.getElementById('cgenAlertScene'),
    compositorEngine: document.getElementById('cgenCompositorEngine'),
    pidAssignment: document.getElementById('cgenPidAssignment'),
    generatedAlertCues: document.getElementById('cgenGeneratedAlertCues'),
    programOutput: document.getElementById('cgenProgramOutput'),
    outputFormat: document.getElementById('cgenOutputFormat'),
    vcodec: document.getElementById('cgenVCodec'),
    acodec: document.getElementById('cgenACodec'),
    hdBitrate: document.getElementById('cgenHdBitrate'),
    videoGop: document.getElementById('cgenVideoGop'),
    videoBFrames: document.getElementById('cgenVideoBFrames'),
    videoPreset: document.getElementById('cgenVideoPreset'),
    videoTune: document.getElementById('cgenVideoTune'),
    videoProfile: document.getElementById('cgenVideoProfile'),
    videoLevel: document.getElementById('cgenVideoLevel'),
    audioEncoderBitrate: document.getElementById('cgenAudioEncoderBitrate'),
    audioProfile: document.getElementById('cgenAudioProfile'),
    audioLevel: document.getElementById('cgenAudioLevel'),
    p720Enabled: document.getElementById('cgenP720Enabled'),
    p720Bitrate: document.getElementById('cgenP720Bitrate'),
    sdEnabled: document.getElementById('cgenSdEnabled'),
    sdBitrate: document.getElementById('cgenSdBitrate'),
    surroundEnabled: document.getElementById('cgenSurroundEnabled'),
    surroundBitrate: document.getElementById('cgenSurroundBitrate'),
    stereoEnabled: document.getElementById('cgenStereoEnabled'),
    stereoBitrate: document.getElementById('cgenStereoBitrate'),
    serviceName: document.getElementById('cgenServiceName'),
    providerName: document.getElementById('cgenProviderName'),
    serviceID: document.getElementById('cgenServiceID'),
    transportStreamID: document.getElementById('cgenTransportStreamID'),
    hdProgram: document.getElementById('cgenHdProgram'),
    hdVideoPID: document.getElementById('cgenHdVideoPID'),
    hdPmtPID: document.getElementById('cgenHdPmtPID'),
    p720Program: document.getElementById('cgenP720Program'),
    p720VideoPID: document.getElementById('cgenP720VideoPID'),
    p720PmtPID: document.getElementById('cgenP720PmtPID'),
    sdProgram: document.getElementById('cgenSdProgram'),
    sdVideoPID: document.getElementById('cgenSdVideoPID'),
    sdPmtPID: document.getElementById('cgenSdPmtPID'),
    stereoProgram: document.getElementById('cgenStereoProgram'),
    stereoAudioPID: document.getElementById('cgenStereoAudioPID'),
    stereoPmtPID: document.getElementById('cgenStereoPmtPID'),
    surroundProgram: document.getElementById('cgenSurroundProgram'),
    surroundAudioPID: document.getElementById('cgenSurroundAudioPID'),
    surroundPmtPID: document.getElementById('cgenSurroundPmtPID'),
    syncHardReset: document.getElementById('cgenSyncHardReset'),
    syncMaxAudioFrames: document.getElementById('cgenSyncMaxAudioFrames'),
    syncSourceBuffer: document.getElementById('cgenSyncSourceBuffer'),
    syncReconnectInitial: document.getElementById('cgenSyncReconnectInitial'),
    syncReconnectMax: document.getElementById('cgenSyncReconnectMax'),
    syncStatusInterval: document.getElementById('cgenSyncStatusInterval'),
    width: document.getElementById('cgenWidth'),
    height: document.getElementById('cgenHeight'),
    fps: document.getElementById('cgenFPS'),
    interlaced: document.getElementById('cgenInterlaced'),
    fieldOrder: document.getElementById('cgenFieldOrder'),
    standard: document.getElementById('cgenStandard'),
    font: document.getElementById('cgenFont'),
    fontWeight: document.getElementById('cgenFontWeight'),
    fontSize: document.getElementById('cgenFontSize'),
    bannerHeight: document.getElementById('cgenBannerHeight'),
    bannerMode: document.getElementById('cgenBannerMode'),
    scrollRepeatMode: document.getElementById('cgenScrollRepeatMode'),
    afterEomRepeats: document.getElementById('cgenAfterEomRepeats'),
    fixedRepeats: document.getElementById('cgenFixedRepeats'),
    standbyMode: document.getElementById('cgenStandbyMode'),
    standbyText: document.getElementById('cgenStandbyText'),
    standbyFontSize: document.getElementById('cgenStandbyFontSize'),
    standbyYPercent: document.getElementById('cgenStandbyYPercent'),
    scrollSpeed: document.getElementById('cgenScrollSpeed'),
    text: document.getElementById('cgenText'),
    textEnabled: document.getElementById('cgenTextEnabled'),
    textFontSize: document.getElementById('cgenTextFontSize'),
    clockEnabled: document.getElementById('cgenClockEnabled'),
    clockFormat: document.getElementById('cgenClockFormat'),
    clockX: document.getElementById('cgenClockX'),
    clockY: document.getElementById('cgenClockY'),
    clockFontSize: document.getElementById('cgenClockFontSize'),
    smpteBars: document.getElementById('cgenSmpteBars'),
    sunnyCat: document.getElementById('cgenSunnyCat'),
};

let bound = false;
let cgenEnabled = true;
let cgenRevision = '';
let feeds = [];
let selectedID = '';
let editorDirty = false;
let renderScheduled = false;
let previewDirty = false;
let metaDirty = false;
let previewStreamFeedID = '';
let previewRetryTimer = 0;
let scenes = [];
let sceneCollectionRevision = '';
let activeScene = null;
let sceneDirty = false;
let cgenCatalog = {
    formats: [],
    video_codecs: [],
    audio_codecs: [],
    video_decoders: [],
    deinterlacers: [],
    devices: [],
    fonts: [],
};
const managedFontFaces = new Map();

function setStatus(text, state = 'ok') {
    if (statusBanner.textContent !== text) {
        statusBanner.textContent = text;
    }
    statusBanner.dataset.state = state;
}

function setText(element, text) {
    if (element && element.textContent !== text) {
        element.textContent = text;
    }
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
    for (const panel of cgenTabPanels) {
        panel.hidden = panel.dataset.cgenPanel !== activeID;
    }
}

function bindCgenTabs() {
    if (!cgenTabs.length || !cgenTabPanels.length) return;
    for (const tab of cgenTabs) {
        tab.addEventListener('click', () => setCgenTab(tab.dataset.cgenTab || ''));
        tab.addEventListener('keydown', (event) => {
            if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
            event.preventDefault();
            const current = cgenTabs.indexOf(tab);
            let next = current;
            if (event.key === 'Home') {
                next = 0;
            } else if (event.key === 'End') {
                next = cgenTabs.length - 1;
            } else {
                const delta = event.key === 'ArrowRight' ? 1 : -1;
                next = (current + delta + cgenTabs.length) % cgenTabs.length;
            }
            const nextTab = cgenTabs[next];
            setCgenTab(nextTab.dataset.cgenTab || '');
            nextTab.focus();
        });
    }
    setCgenTab(cgenTabs.find((tab) => tab.getAttribute('aria-selected') === 'true')?.dataset.cgenTab);
}

function scheduleRender({ preview: needsPreview = true, meta: needsMeta = true } = {}) {
    previewDirty = previewDirty || needsPreview;
    metaDirty = metaDirty || needsMeta;
    if (renderScheduled) return;
    renderScheduled = true;
    window.requestAnimationFrame(() => {
        renderScheduled = false;
        if (previewDirty) {
            previewDirty = false;
            renderPreview();
        }
        if (metaDirty) {
            metaDirty = false;
            renderMeta();
        }
    });
}

function selected() {
    return feeds.find((feed) => feed.id === selectedID) || feeds[0] || null;
}

function value(key, fallback = '') {
    const field = fields[key];
    if (!field) return fallback;
    if (field.type === 'checkbox') return field.checked;
    return String(field.value || fallback).trim();
}

function inputTypeValue() {
    const type = String(fields.programInputType?.value || 'uri_or_file').trim().toLowerCase();
    if (['uri_or_file', 'device', 'dummy', 'stream'].includes(type)) return type;
    return 'uri_or_file';
}

function updateProgramInputVisibility() {
    const type = inputTypeValue();
    document.querySelectorAll('[data-cgen-input-option]').forEach((element) => {
        const supported = String(element.dataset.cgenInputOption || '').split(/\s+/).filter(Boolean);
        element.hidden = !supported.includes(type);
    });
    const hardwareDecoderEnabled = ['uri_or_file', 'stream', 'device'].includes(type) && fields.hardwareDecoderEnabled?.checked === true;
    if (fields.hardwareDecoder) fields.hardwareDecoder.disabled = !hardwareDecoderEnabled;
}

const fallbackDeinterlacers = [
    { algorithm: 'yadif', backend: 'software', label: 'YADIF, software', available: true, parity_control: true },
    { algorithm: 'bwdif', backend: 'software', label: 'BWDIF, software', available: false, parity_control: true, reason: 'The runtime catalog did not report an in-process BWDIF filter.' },
    { algorithm: 'yadif', backend: 'cuda', label: 'YADIF, NVIDIA CUDA', available: false, parity_control: false, reason: 'The runtime catalog did not report a YADIF CUDA filter.' },
    { algorithm: 'bwdif', backend: 'cuda', label: 'BWDIF, NVIDIA CUDA', available: false, parity_control: false, reason: 'The runtime catalog did not report a BWDIF CUDA filter.' },
    { algorithm: 'bwdif', backend: 'vulkan', label: 'BWDIF, Vulkan', available: false, parity_control: false, reason: 'The runtime catalog did not report a BWDIF Vulkan filter.' },
    { algorithm: 'motion_adaptive', backend: 'software', label: 'Motion adaptive, software', available: true, parity_control: true },
    { algorithm: 'motion_adaptive', backend: 'vaapi', label: 'Motion adaptive, VA-API', available: false, parity_control: false },
    { algorithm: 'motion_adaptive', backend: 'quicksync', label: 'Motion adaptive, Intel Quick Sync', available: false, parity_control: false },
    { algorithm: 'motion_adaptive', backend: 'd3d11', label: 'Motion compensated, Direct3D 11', available: false, parity_control: false },
    { algorithm: 'motion_adaptive', backend: 'opengl', label: 'Motion adaptive, OpenGL', available: false, parity_control: false },
];

function deinterlacerVariants() {
    return Array.isArray(cgenCatalog.deinterlacers) && cgenCatalog.deinterlacers.length
        ? cgenCatalog.deinterlacers
        : fallbackDeinterlacers;
}

function updateDeinterlaceControls({ selectFallback = false } = {}) {
    if (!fields.deinterlaceAlgorithm || !fields.deinterlaceBackend) return;
    const variants = deinterlacerVariants();
    const algorithm = String(fields.deinterlaceAlgorithm.value || 'yadif');
    for (const algorithmOption of fields.deinterlaceAlgorithm.options) {
        const hasAvailableVariant = variants.some((entry) =>
            entry.algorithm === algorithmOption.value && entry.available === true);
        algorithmOption.disabled = !hasAvailableVariant;
        const baseLabel = algorithmOption.value === 'motion_adaptive'
            ? 'Motion adaptive'
            : algorithmOption.value.toUpperCase();
        algorithmOption.textContent = hasAvailableVariant ? baseLabel : `${baseLabel} (unavailable)`;
    }

    const backendDefinitions = [
        ['auto', 'Best available'],
        ['software', 'Software'],
        ['vaapi', 'VA-API'],
        ['quicksync', 'Intel Quick Sync'],
        ['d3d11', 'Direct3D 11'],
        ['cuda', 'NVIDIA CUDA'],
        ['vulkan', 'Vulkan'],
        ['opengl', 'OpenGL'],
    ];
    const previousBackend = String(fields.deinterlaceBackend.value || 'software');
    fields.deinterlaceBackend.replaceChildren();
    for (const [backend, label] of backendDefinitions) {
        const candidates = variants.filter((entry) => entry.algorithm === algorithm &&
            (backend === 'auto' || entry.backend === backend));
        const available = candidates.some((entry) => entry.available === true);
        const backendOption = option(backend, available ? label : `${label} (unavailable)`);
        backendOption.disabled = !available;
        fields.deinterlaceBackend.append(backendOption);
    }
    const previousOption = Array.from(fields.deinterlaceBackend.options)
        .find((backendOption) => backendOption.value === previousBackend);
    if (previousOption && (!selectFallback || !previousOption.disabled)) {
        fields.deinterlaceBackend.value = previousBackend;
    } else {
        const availableOption = Array.from(fields.deinterlaceBackend.options)
            .find((backendOption) => !backendOption.disabled);
        if (availableOption) fields.deinterlaceBackend.value = availableOption.value;
    }

    const backend = String(fields.deinterlaceBackend.value || 'software');
    const selectedVariants = variants.filter((entry) =>
        entry.algorithm === algorithm && (backend === 'auto' || entry.backend === backend));
    const availableVariants = selectedVariants.filter((entry) => entry.available === true);
    const parityControl = availableVariants.length > 0 &&
        availableVariants.every((entry) => entry.parity_control === true);
    if (fields.deinterlaceParity) {
        fields.deinterlaceParity.disabled = !parityControl;
        if (!parityControl) fields.deinterlaceParity.value = 'auto';
    }
    const exactVariant = selectedVariants.find((entry) => entry.backend === backend);
    if (deinterlaceHint) {
        if (backend === 'auto' && availableVariants.length) {
            deinterlaceHint.textContent = `Available choices: ${availableVariants.map((entry) => entry.label).join(', ')}. Field-rate output preserves full motion cadence.`;
        } else if (exactVariant?.available) {
            deinterlaceHint.textContent = `${exactVariant.label} is available. Field-rate output preserves full motion cadence.`;
        } else {
            deinterlaceHint.textContent = exactVariant?.reason || 'This deinterlacing combination is unavailable on the current runtime.';
        }
    }
}

function updateAudioTopologyVisibility() {
    const topology = String(fields.audioTopology?.value || 'force_layout').trim().toLowerCase();
    document.querySelectorAll('[data-cgen-audio-option]').forEach((element) => {
        const supported = String(element.dataset.cgenAudioOption || '').split(/\s+/).filter(Boolean);
        element.hidden = !supported.includes(topology);
    });
    const preserve = topology === 'preserve_native_tracks';
    if (fields.acodec) {
        if (preserve) {
            ensureSelectOption(fields.acodec, 'match_input', 'Match Input');
            fields.acodec.value = 'match_input';
        } else if (fields.acodec.value === 'match_input') {
            ensureSelectOption(fields.acodec, 'avenc_aac', 'AAC - libav (avenc_aac)');
            fields.acodec.value = 'avenc_aac';
        }
        fields.acodec.disabled = preserve;
    }
    if (fields.audioEncoderBitrate) fields.audioEncoderBitrate.disabled = preserve;
    outputsBody?.querySelectorAll('[data-output-field="audio_codec"]').forEach((select) => {
        if (preserve) {
            select.value = 'match_input';
        } else if (select.value === 'match_input') {
            select.value = 'aac';
        }
        select.disabled = preserve;
    });
}

function updatePidAssignmentVisibility() {
    const mode = String(fields.pidAssignment?.value || 'source');
    const manual = mode === 'manual';
    const source = mode === 'source';
    [
        fields.hdVideoPID, fields.hdPmtPID,
        fields.p720VideoPID, fields.p720PmtPID,
        fields.sdVideoPID, fields.sdPmtPID,
        fields.stereoAudioPID, fields.stereoPmtPID,
        fields.surroundAudioPID, fields.surroundPmtPID,
    ].forEach((field) => {
        if (field) field.disabled = !manual;
    });
    [
        fields.transportStreamID,
        fields.hdProgram,
        fields.p720Program,
        fields.sdProgram,
        fields.stereoProgram,
        fields.surroundProgram,
    ].forEach((field) => {
        if (field) field.disabled = source;
    });
}

function pidEditorValue(key, fallback) {
    return value('pidAssignment', 'auto') === 'manual' ? value(key, fallback) : 'auto';
}

function option(value, label) {
    const entry = document.createElement('option');
    entry.value = value;
    entry.textContent = label;
    return entry;
}

function destinationSelect(value) {
    const select = document.createElement('select');
    select.dataset.outputField = 'destination';
    select.append(
        option('mpeg_ts_udp', 'MPEG-TS / UDP'),
        option('mpeg_ts_srt', 'MPEG-TS / SRT'),
        option('rtp', 'RTP endpoints'),
        option('rtmp', 'RTMP'),
        option('file', 'File'),
    );
    select.value = value || 'mpeg_ts_udp';
    return select;
}

function codecSelect(kind, value) {
    const select = document.createElement('select');
    select.dataset.outputField = kind;
    if (kind === 'video_codec') {
        select.append(option('h264', 'H.264'), option('h265', 'H.265'), option('mpeg2', 'MPEG-2'));
    } else {
        select.append(option('aac', 'AAC'), option('ac3', 'AC-3'), option('mp2', 'MP2'), option('match_input', 'Match input'));
    }
    select.value = value || (kind === 'video_codec' ? 'h264' : 'aac');
    return select;
}

function rateControlSelect(value) {
    const select = document.createElement('select');
    select.dataset.outputField = 'rate_control';
    select.style.marginTop = '4px';
    select.append(option('cbr', 'CBR'), option('vbr', 'VBR target/max'));
    select.value = value || 'cbr';
    return select;
}

function endpointInput(value, placeholder, field) {
    const input = document.createElement('input');
    input.type = 'text';
    input.autocomplete = 'off';
    input.spellcheck = false;
    input.placeholder = placeholder;
    input.value = String(value || '');
    input.dataset.outputField = field;
    return input;
}

function numericOutputInput(value, placeholder, field, min, step = '1') {
    const input = document.createElement('input');
    input.type = 'number';
    input.min = String(min);
    input.step = step;
    input.placeholder = placeholder;
    input.value = String(value || '');
    input.dataset.outputField = field;
    input.style.marginTop = '4px';
    return input;
}

function setRtpEndpointVisibility(row) {
    const isRtp = row.querySelector('[data-output-field="destination"]')?.value === 'rtp';
    const audioEndpoints = row.querySelector('[data-output-field="audio_urls"]');
    if (audioEndpoints) audioEndpoints.hidden = !isRtp;
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
    const id = document.createElement('input');
    id.type = 'text';
    id.maxLength = 96;
    id.value = output.id || 'output';
    id.dataset.outputField = 'id';
    idCell.append(id);

    const destinationCell = document.createElement('td');
    const destination = destinationSelect(output.destination);
    destinationCell.append(destination);

    const endpointCell = document.createElement('td');
    const endpoint = endpointInput(output.url || output.video_url, '${OUTPUT_URL}', 'url');
    const audioEndpoints = endpointInput(output.audio_urls, 'Audio URLs, comma separated', 'audio_urls');
    audioEndpoints.style.marginTop = '4px';
    endpointCell.append(endpoint, audioEndpoints);

    const videoCell = document.createElement('td');
    const videoCodec = codecSelect('video_codec', normalizeVideoCodec(output.video_codec));
    const videoBitrate = numericOutputInput(output.video_bitrate_kbps || '8000', 'video kbps', 'video_bitrate_kbps', 100);
    const videoMaxBitrate = numericOutputInput(output.video_max_bitrate_kbps || '', 'max kbps (VBR)', 'video_max_bitrate_kbps', 100);
    const gop = numericOutputInput(output.gop_frames || '60', 'GOP frames', 'gop_frames', 1);
    videoCell.append(videoCodec, rateControlSelect(output.rate_control), videoBitrate, videoMaxBitrate, gop);
    const audioCell = document.createElement('td');
    const audioCodec = codecSelect('audio_codec', normalizeAudioCodec(output.audio_codec));
    const audioBitrate = numericOutputInput(output.audio_bitrate_kbps || '192', 'audio kbps', 'audio_bitrate_kbps', 32, '8');
    const sampleRate = numericOutputInput(output.sample_rate || '48000', 'sample rate', 'sample_rate', 8000);
    audioCell.append(audioCodec, audioBitrate, sampleRate);

    const removeCell = document.createElement('td');
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.className = 'btn-danger';
    remove.textContent = 'Remove';
    remove.addEventListener('click', () => {
        row.remove();
        editorDirty = true;
        scheduleRender({ preview: false, meta: true });
    });
    removeCell.append(remove);

    destination.addEventListener('change', () => setRtpEndpointVisibility(row));
    row.append(enabledCell, idCell, destinationCell, endpointCell, videoCell, audioCell, removeCell);
    setRtpEndpointVisibility(row);
    return row;
}

function renderOutputsEditor(outputs = []) {
    if (!outputsBody) return;
    outputsBody.replaceChildren(...outputs.map((output) => outputRow(output)));
}

function readOutputsEditor() {
    if (!outputsBody) return [];
    return Array.from(outputsBody.querySelectorAll('[data-output-row]')).map((row, index) => {
        const read = (name) => row.querySelector(`[data-output-field="${name}"]`);
        const enabled = read('enabled');
        const destination = String(read('destination')?.value || 'mpeg_ts_udp');
        const endpoint = String(read('url')?.value || '').trim();
        const result = {
            ...(row._cgenOutputBase || {}),
            id: sanitizeID(read('id')?.value || `output-${index + 1}`),
            enabled: enabled?.checked !== false,
            destination,
            url: destination === 'rtp' ? '' : endpoint,
            video_url: destination === 'rtp' ? endpoint : '',
            audio_urls: destination === 'rtp' ? String(read('audio_urls')?.value || '').trim() : '',
            video_codec: String(read('video_codec')?.value || 'h264'),
            audio_codec: String(read('audio_codec')?.value || 'aac'),
            video_bitrate_kbps: String(read('video_bitrate_kbps')?.value || '8000'),
            video_max_bitrate_kbps: String(read('video_max_bitrate_kbps')?.value || ''),
            rate_control: String(read('rate_control')?.value || 'cbr'),
            audio_bitrate_kbps: String(read('audio_bitrate_kbps')?.value || '192'),
            sample_rate: String(read('sample_rate')?.value || '48000'),
            gop_frames: String(read('gop_frames')?.value || '60'),
        };
        return result;
    });
}

function normalizeVideoCodec(codec) {
    const value = String(codec || '').toLowerCase();
    if (value.includes('265') || value.includes('hevc')) return 'h265';
    if (value.includes('mpeg2')) return 'mpeg2';
    return 'h264';
}

function normalizeAudioCodec(codec) {
    const value = String(codec || '').toLowerCase();
    if (value === 'match_input' || value.includes('match')) return 'match_input';
    if (value.includes('ac3')) return 'ac3';
    if (value.includes('mp2')) return 'mp2';
    return 'aac';
}

function legacyOutputFromFeed(feed) {
    if (!feed?.program_output_url) return [];
    return [{
        id: 'legacy-main',
        enabled: feed.enabled !== false,
        destination: String(feed.program_output_url).toLowerCase().startsWith('srt:') ? 'mpeg_ts_srt' : 'mpeg_ts_udp',
        url: feed.program_output_url,
        video_codec: normalizeVideoCodec(feed.vcodec),
        audio_codec: normalizeAudioCodec(feed.acodec),
        video_bitrate_kbps: feed.video_bitrate_kbps || feed.hd_bitrate_kbps || '12000',
        audio_bitrate_kbps: feed.audio_bitrate_kbps || feed.stereo_bitrate_kbps || '192',
        sample_rate: '48000',
        gop_frames: feed.video_gop || '15',
        rate_control: 'cbr',
    }];
}

function validatePipelineEditor(feed) {
    const alertFeed = String(feed.alert_feed_id || '').trim();
    if (!alertFeed || alertFeed === '*') throw new Error('A concrete alert feed ID is required. Wildcards are not supported.');
    if (feed.program_input_type === 'device' && !String(feed.device_id || '').trim()) {
        throw new Error('Select a persistent capture device ID.');
    }
    if (['uri_or_file', 'stream'].includes(feed.program_input_type) && !String(feed.program_input_url || '').trim()) {
        throw new Error('A program URL, file, or environment reference is required.');
    }
    if (feed.program_input_type !== 'dummy') {
        const variants = deinterlacerVariants().filter((entry) =>
            entry.algorithm === feed.deinterlace_algorithm &&
            (feed.deinterlace_backend === 'auto' || entry.backend === feed.deinterlace_backend));
        if (!variants.some((entry) => entry.available === true)) {
            const reason = variants.find((entry) => entry.reason)?.reason;
            throw new Error(reason || 'The selected deinterlacing algorithm and backend are unavailable.');
        }
    }
    if (!protectedAlertScene(feed.alert_scene_id)) throw new Error('Program_Passthrough and Standby cannot be selected as alert scenes.');
    if (feed.audio_topology === 'preserve_native_tracks' && !preserveNativeAudioAvailable()) {
        throw new Error('Preserve-native audio is unavailable in the current media backend. Select Force layout.');
    }
    validateGain(feed.audio_idle_program_gain_db, 'Idle program gain');
    validateGain(feed.audio_alert_program_gain_db, 'Alert program gain');
    validateGain(feed.audio_alert_gain_db, 'Alert audio gain');
    if (!Array.isArray(feed.outputs) || !feed.outputs.some((output) => output.enabled !== false)) {
        throw new Error('At least one encoder destination must be enabled.');
    }
    const ids = new Set();
    for (const output of feed.outputs) {
        if (ids.has(output.id)) throw new Error(`Output ID ${output.id} is duplicated.`);
        ids.add(output.id);
        const endpoint = output.destination === 'rtp' ? output.video_url : output.url;
        if (output.enabled !== false && !String(endpoint || '').trim()) {
            throw new Error(`Output ${output.id} requires an endpoint reference.`);
        }
        if (output.enabled !== false && output.destination === 'rtp' && !String(output.audio_urls || '').trim()) {
            throw new Error(`RTP output ${output.id} requires explicit audio endpoints.`);
        }
        if (output.enabled !== false && output.destination === 'rtmp' && (output.video_codec !== 'h264' || output.audio_codec !== 'aac')) {
            throw new Error(`RTMP output ${output.id} requires H.264 video and AAC audio.`);
        }
        if (output.enabled !== false && output.rate_control === 'vbr') {
            const target = Number(output.video_bitrate_kbps);
            const max = Number(output.video_max_bitrate_kbps);
            if (!Number.isFinite(max) || max < target) {
                throw new Error(`VBR output ${output.id} requires a max bitrate at or above its target bitrate.`);
            }
        }
        if (feed.audio_topology === 'preserve_native_tracks' && output.audio_codec !== 'match_input') {
            throw new Error(`Output ${output.id} must use Match input while native tracks are preserved.`);
        }
        if (feed.audio_topology !== 'preserve_native_tracks' && output.audio_codec === 'match_input') {
            throw new Error(`Output ${output.id} requires AAC, AC-3, or MP2 in forced-layout mode.`);
        }
    }
}

function validateGain(raw, label) {
    const value = String(raw ?? '').trim().toLowerCase();
    if (value === 'muted') return;
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < -60 || numeric > 12) {
        throw new Error(`${label} must be muted or a number from -60 dB through +12 dB.`);
    }
}

function optionLabelForValue(value) {
    const text = String(value || '').trim();
    return text ? `${text} (current/custom)` : '';
}

function ensureSelectOption(select, value, label = '') {
    if (!select || select.tagName !== 'SELECT') return;
    const text = String(value ?? '').trim();
    if (!text) return;
    const exists = Array.from(select.options).some((option) => option.value === text);
    if (exists) return;
    const option = document.createElement('option');
    option.value = text;
    option.textContent = label || optionLabelForValue(text);
    option.dataset.custom = 'true';
    if (select.id === 'cgenFont') {
        option.style.fontFamily = fontCssStack(text);
    }
    select.appendChild(option);
}

function setValue(key, raw) {
    const field = fields[key];
    if (!field) return;
    if (field.type === 'checkbox') {
        field.checked = Boolean(raw);
    } else {
        ensureSelectOption(field, raw);
        field.value = raw ?? '';
    }
    if (key === 'font') {
        updateFontPreview();
    }
}

function sanitizeID(value) {
    const cleaned = String(value || '').trim().replace(/[^a-zA-Z0-9_-]+/g, '-').replace(/^-+|-+$/g, '');
    return cleaned || `cgen-${Date.now().toString(36)}`;
}

function readEditor() {
    const id = sanitizeID(value('id'));
    const outputs = readOutputsEditor();
    const primaryOutput = outputs.find((output) => output.enabled !== false && ['mpeg_ts_udp', 'mpeg_ts_srt'].includes(output.destination));
    const dummyScanMode = value('dummyScanMode', 'progressive');
    const alertFeedID = value('priorityFeed', id);
    const captions = value('captionsPass') ? 'pass' : 'drop';
    const scte35 = value('scte35Pass') ? 'pass' : 'drop';
    const scte104 = value('scte104Pass') ? 'pass' : 'drop';
    const audioTopology = value('audioTopology', 'force_layout');
    const forceLayout = value('forcedLayout', 'stereo');
    const idleProgramGain = value('idleProgramGain', '0');
    const alertProgramGain = value('alertProgramGain', 'muted');
    const alertGain = value('alertGain', '0');
    const transitionMS = value('audioTransition', '20');
    const alertSceneID = value('alertScene', 'Standard_Crawl');
    const compositorEngine = value('compositorEngine', 'legacy');
    const audioMappings = [];
    if (value('stereoEnabled')) audioMappings.push({ track_id: 'stereo', pid: pidEditorValue('stereoAudioPID', '257') });
    if (value('surroundEnabled')) audioMappings.push({ track_id: 'surround_51', pid: pidEditorValue('surroundAudioPID', '258') });
    const programMapping = {
        mode: value('pidAssignment', 'source'),
        transport_stream_id: value('transportStreamID', '1'),
        programs: [{
            number: value('hdProgram', '1'),
            service_name: value('serviceName', 'Haze CGEN'),
            provider_name: value('providerName', 'Haze'),
            pmt_pid: pidEditorValue('hdPmtPID', '4096'),
            video_pid: pidEditorValue('hdVideoPID', '256'),
            audio: audioMappings,
            scte35: {
                input: scte35,
                generated_alert_cues: value('generatedAlertCues'),
                pid: 'auto',
            },
        }],
    };
    return {
        id,
        name: value('name', id),
        enabled: value('enabled'),
        mode: value('mode', 'release'),
        smpte_bars: value('smpteBars'),
        sunny_cat: value('sunnyCat'),
        program_input_type: inputTypeValue(),
        program_input_url: value('programInput'),
        program_input_format: value('programInputFormat', 'mpegts'),
        hardware_decoder_enabled: value('hardwareDecoderEnabled'),
        hardware_decoder: value('hardwareDecoder'),
        decoder_preference: value('hardwareDecoderEnabled') ? value('hardwareDecoder', 'auto') : 'auto',
        deinterlace_algorithm: value('deinterlaceAlgorithm', 'yadif'),
        deinterlace_backend: value('deinterlaceBackend', 'software'),
        deinterlace_cadence: value('deinterlaceCadence', 'field'),
        deinterlace_parity: value('deinterlaceParity', 'auto'),
        device_backend: value('deviceBackend', 'v4l2'),
        device_id: value('deviceID'),
        dummy_width: value('dummyWidth', '720'),
        dummy_height: value('dummyHeight', '480'),
        dummy_fps: value('dummyFPS', '30000/1001'),
        dummy_scan_mode: dummyScanMode,
        dummy_interlaced: dummyScanMode !== 'progressive',
        dummy_field_order: dummyScanMode === 'interlaced_bff' ? 'bff' : 'tff',
        dummy_background: value('dummyBackground', '#000000FF'),
        priority_feed_id: alertFeedID,
        alert_feed_id: alertFeedID,
        audio_source: value('audioSource', 'priority'),
        priority_input_format: 'priority-audio',
        ancillary_captions: captions,
        ancillary_scte35: scte35,
        ancillary_scte104: scte104,
        audio_topology: audioTopology,
        audio_force_layout: forceLayout,
        idle_program_gain_db: idleProgramGain,
        alert_program_gain_db: alertProgramGain,
        alert_gain_db: alertGain,
        audio_idle_program_gain_db: idleProgramGain,
        audio_alert_program_gain_db: alertProgramGain,
        audio_alert_gain_db: alertGain,
        audio_transition_ms: transitionMS,
        compositor_engine: compositorEngine,
        alert_scene_id: alertSceneID,
        pid_assignment: value('pidAssignment', 'auto'),
        generated_alert_cues: value('generatedAlertCues'),
        program_input: {
            type: inputTypeValue(),
            url: value('programInput'),
            format: value('programInputFormat', 'mpegts'),
            hardware_decoder_enabled: value('hardwareDecoderEnabled'),
            hardware_decoder: value('hardwareDecoder'),
            deinterlace_algorithm: value('deinterlaceAlgorithm', 'yadif'),
            deinterlace_backend: value('deinterlaceBackend', 'software'),
            deinterlace_cadence: value('deinterlaceCadence', 'field'),
            deinterlace_parity: value('deinterlaceParity', 'auto'),
            device_backend: value('deviceBackend', 'v4l2'),
            device_id: value('deviceID'),
            width: value('dummyWidth', '720'),
            height: value('dummyHeight', '480'),
            fps: value('dummyFPS', '30000/1001'),
            interlaced: dummyScanMode !== 'progressive',
            field_order: dummyScanMode === 'interlaced_bff' ? 'bff' : 'tff',
            background: value('dummyBackground', '#000000FF'),
        },
        alert: { feed_id: alertFeedID },
        ancillary: { captions, scte35, scte104 },
        audio_routing: {
            topology: audioTopology,
            force_layout: forceLayout,
            idle_program_gain_db: idleProgramGain,
            alert_program_gain_db: alertProgramGain,
            alert_gain_db: alertGain,
            transition_ms: transitionMS,
        },
        compositor: { alert_scene_id: alertSceneID, engine: compositorEngine },
        program_mapping: programMapping,
        outputs,
        program_output_url: primaryOutput?.url || value('programOutput'),
        program_output_format: value('outputFormat', 'mpegts'),
        vcodec: value('vcodec', 'avenc_mpeg2video'),
        acodec: value('acodec', 'avenc_ac3'),
        video_bitrate_kbps: value('hdBitrate', '12000'),
        audio_bitrate_kbps: value('stereoBitrate', '192'),
        video_encoder_codec: value('vcodec', 'avenc_mpeg2video'),
        video_encoder_bitrate_kbps: value('hdBitrate', '12000'),
        video_gop: value('videoGop', '15'),
        video_bframes: value('videoBFrames', '0'),
        video_preset: value('videoPreset'),
        video_tune: value('videoTune'),
        video_profile: value('videoProfile'),
        video_level: value('videoLevel'),
        audio_encoder_codec: value('acodec', 'avenc_ac3'),
        audio_encoder_bitrate_kbps: value('audioEncoderBitrate', value('stereoBitrate', '192')),
        audio_profile: value('audioProfile'),
        audio_level: value('audioLevel'),
        service_name: value('serviceName'),
        provider_name: value('providerName'),
        service_id: value('serviceID'),
        transport_stream_id: value('transportStreamID'),
        hd_enabled: 'auto',
        hd_bitrate_kbps: value('hdBitrate', '12000'),
        hd_program: value('hdProgram', '1'),
        hd_video_pid: pidEditorValue('hdVideoPID', '256'),
        hd_pmt_pid: pidEditorValue('hdPmtPID', '4096'),
        p720_enabled: value('p720Enabled'),
        p720_bitrate_kbps: value('p720Bitrate', '8000'),
        p720_program: value('p720Program', '2'),
        p720_video_pid: pidEditorValue('p720VideoPID', '288'),
        p720_pmt_pid: pidEditorValue('p720PmtPID', '4097'),
        sd_enabled: value('sdEnabled'),
        sd_bitrate_kbps: value('sdBitrate', '5000'),
        sd_program: value('sdProgram', '3'),
        sd_video_pid: pidEditorValue('sdVideoPID', '320'),
        sd_pmt_pid: pidEditorValue('sdPmtPID', '4098'),
        surround_enabled: value('surroundEnabled'),
        surround_bitrate_kbps: value('surroundBitrate', '384'),
        surround_program: value('surroundProgram', '1'),
        surround_audio_pid: pidEditorValue('surroundAudioPID', '258'),
        surround_pmt_pid: pidEditorValue('surroundPmtPID', '4096'),
        stereo_enabled: value('stereoEnabled'),
        stereo_bitrate_kbps: value('stereoBitrate', '192'),
        stereo_program: value('stereoProgram', '1'),
        stereo_audio_pid: pidEditorValue('stereoAudioPID', '257'),
        stereo_pmt_pid: pidEditorValue('stereoPmtPID', '4096'),
        width: value('width', '1920'),
        height: value('height', '1080'),
        fps: value('fps', '30000/1001'),
        interlaced: value('interlaced'),
        field_order: value('fieldOrder', 'tff'),
        standard: value('standard', 'atsc'),
        audio_idle: value('audioIdle', 'source'),
        audio_alert_mode: 'replace',
        mute_standby_routine: value('muteStandbyRoutine', true),
        banner_mode: value('bannerMode', 'auto'),
        ticker_height: value('bannerHeight', '128'),
        font: value('font', 'Arial'),
        font_weight: value('fontWeight', 'regular'),
        font_size: value('fontSize', '58'),
        scroll_speed: value('scrollSpeed', '8'),
        scroll_repeat_mode: value('scrollRepeatMode', 'until_audio_end'),
        after_eom_repeats: value('afterEomRepeats', '1'),
        fixed_repeats: value('fixedRepeats', '1'),
        banner_background_enabled: true,
        banner_height: value('bannerHeight', '128'),
        standby_mode: value('standbyMode', 'banner'),
        standby_text: value('standbyText', 'Emergency Alert Details Channel'),
        standby_font_size: value('standbyFontSize', value('fontSize', '58')),
        standby_y_percent: value('standbyYPercent', '10'),
        text_enabled: value('textEnabled'),
        text: fields.text.value,
        text_font_size: value('textFontSize', '58'),
        text_color: '#ffffff',
        clock_enabled: value('clockEnabled'),
        clock_format: value('clockFormat', 'Jan 02 15:04:05'),
        clock_x: value('clockX', '48'),
        clock_y: value('clockY', '48'),
        clock_font_size: value('clockFontSize', '30'),
        clock_color: '#ffffff',
        sync_hard_reset_ms: value('syncHardReset', '250'),
        sync_max_audio_frames_per_video: value('syncMaxAudioFrames', '8'),
        sync_source_buffer_ms: value('syncSourceBuffer', '240'),
        sync_reconnect_initial_ms: value('syncReconnectInitial', '500'),
        sync_reconnect_max_ms: value('syncReconnectMax', '10000'),
        sync_status_interval_ms: value('syncStatusInterval', '750'),
    };
}

function writeEditor(feed) {
    if (!feed) return;
    const mappedProgram = Array.isArray(feed.program_mapping?.programs) ? feed.program_mapping.programs[0] : null;
    const mappedAudio = Array.isArray(mappedProgram?.audio) ? mappedProgram.audio : [];
    const mappedAudioPID = (trackID, fallback) => mappedAudio.find((stream) => stream.track_id === trackID)?.pid || fallback;
    setValue('id', feed.id);
    setValue('name', feed.name || feed.id);
    setValue('enabled', Boolean(feed.enabled));
    setValue('mode', feed.mode || 'release');
    setValue('smpteBars', Boolean(feed.smpte_bars));
    setValue('sunnyCat', Boolean(feed.sunny_cat));
    setValue('programInputType', feed.program_input_type || 'uri_or_file');
    setValue('programInput', feed.program_input_url || '');
    setValue('programInputFormat', feed.program_input_format || 'mpegts');
    setValue('hardwareDecoderEnabled', Boolean(feed.hardware_decoder_enabled));
    setValue('hardwareDecoder', feed.hardware_decoder || '');
    setValue('deinterlaceAlgorithm', feed.deinterlace_algorithm || 'yadif');
    setValue('deinterlaceBackend', feed.deinterlace_backend || 'software');
    setValue('deinterlaceCadence', feed.deinterlace_cadence || 'field');
    setValue('deinterlaceParity', feed.deinterlace_parity || 'auto');
    updateDeinterlaceControls();
    setValue('deviceBackend', feed.device_backend || 'v4l2');
    setValue('deviceID', feed.device_id || '');
    setValue('dummyWidth', feed.dummy_width || '720');
    setValue('dummyHeight', feed.dummy_height || '480');
    setValue('dummyFPS', feed.dummy_fps || '30000/1001');
    const dummyScanMode = feed.dummy_scan_mode || (feed.dummy_interlaced
        ? (feed.dummy_field_order === 'bff' ? 'interlaced_bff' : 'interlaced_tff')
        : 'progressive');
    setValue('dummyScanMode', dummyScanMode);
    setValue('dummyBackground', feed.dummy_background || '#000000FF');
    const configuredAlertFeed = feed.alert_feed_id || feed.priority_feed_id || feed.id;
    setValue('priorityFeed', configuredAlertFeed === '*' ? feed.id : configuredAlertFeed);
    setValue('audioSource', feed.audio_source || 'priority');
    setValue('audioIdle', feed.audio_idle || 'source');
    setValue('muteStandbyRoutine', feed.mute_standby_routine !== false);
    setValue('captionsPass', feed.ancillary_captions === 'pass' || feed.captions_passthrough === true);
    setValue('scte35Pass', feed.ancillary_scte35 === 'pass' || feed.scte35_passthrough === true);
    setValue('scte104Pass', feed.ancillary_scte104 === 'pass' || feed.scte104_passthrough === true);
    setValue('audioTopology', feed.audio_topology || 'force_layout');
    setValue('forcedLayout', feed.audio_force_layout || feed.force_layout || 'stereo');
    setValue('idleProgramGain', feed.idle_program_gain_db ?? feed.audio_idle_program_gain_db ?? '0');
    setValue('alertProgramGain', feed.alert_program_gain_db ?? feed.audio_alert_program_gain_db ?? 'muted');
    setValue('alertGain', feed.alert_gain_db ?? feed.audio_alert_gain_db ?? '0');
    setValue('audioTransition', feed.audio_transition_ms || '20');
    setValue('compositorEngine', feed.compositor_engine || 'legacy');
    setValue('alertScene', feed.alert_scene_id || 'Standard_Crawl');
    const hasAutoPID = [mappedProgram?.video_pid, mappedProgram?.pmt_pid, ...mappedAudio.map((stream) => stream.pid)]
        .some((pid) => String(pid || '').toLowerCase() === 'auto');
    setValue(
        'pidAssignment',
        feed.program_mapping?.mode || feed.pid_assignment || (hasAutoPID ? 'auto' : 'manual'),
    );
    setValue('generatedAlertCues', Boolean(feed.generated_alert_cues ?? mappedProgram?.scte35?.generated_alert_cues));
    setValue('programOutput', feed.program_output_url || '');
    setValue('outputFormat', feed.program_output_format || 'mpegts');
    setValue('vcodec', feed.vcodec || 'avenc_mpeg2video');
    setValue('acodec', feed.acodec || 'avenc_ac3');
    setValue('hdBitrate', feed.hd_bitrate_kbps || feed.video_bitrate_kbps || '12000');
    setValue('videoGop', feed.video_gop || '15');
    setValue('videoBFrames', feed.video_bframes || '0');
    setValue('videoPreset', feed.video_preset || '');
    setValue('videoTune', feed.video_tune || '');
    setValue('videoProfile', feed.video_profile || '');
    setValue('videoLevel', feed.video_level || '');
    setValue('audioEncoderBitrate', feed.audio_encoder_bitrate_kbps || feed.stereo_bitrate_kbps || feed.audio_bitrate_kbps || '192');
    setValue('audioProfile', feed.audio_profile || '');
    setValue('audioLevel', feed.audio_level || '');
    setValue('serviceName', mappedProgram?.service_name || feed.service_name || '');
    setValue('providerName', mappedProgram?.provider_name || feed.provider_name || '');
    setValue('serviceID', feed.service_id || '1');
    setValue('transportStreamID', feed.program_mapping?.transport_stream_id || feed.transport_stream_id || '1');
    setValue('hdProgram', mappedProgram?.number || feed.hd_program || '1');
    setValue('hdVideoPID', mappedProgram?.video_pid || feed.hd_video_pid || '256');
    setValue('hdPmtPID', mappedProgram?.pmt_pid || feed.hd_pmt_pid || '4096');
    setValue('p720Enabled', Boolean(feed.p720_enabled));
    setValue('p720Bitrate', feed.p720_bitrate_kbps || '8000');
    setValue('p720Program', feed.p720_program || '2');
    setValue('p720VideoPID', feed.p720_video_pid || '288');
    setValue('p720PmtPID', feed.p720_pmt_pid || '4097');
    setValue('sdEnabled', Boolean(feed.sd_enabled));
    setValue('sdBitrate', feed.sd_bitrate_kbps || '5000');
    setValue('sdProgram', feed.sd_program || '3');
    setValue('sdVideoPID', feed.sd_video_pid || '320');
    setValue('sdPmtPID', feed.sd_pmt_pid || '4098');
    setValue('surroundEnabled', feed.surround_enabled !== false);
    setValue('surroundBitrate', feed.surround_bitrate_kbps || '384');
    setValue('surroundProgram', feed.surround_program || '1');
    setValue('surroundAudioPID', mappedAudioPID('surround_51', feed.surround_audio_pid || '258'));
    setValue('surroundPmtPID', mappedProgram?.pmt_pid || feed.surround_pmt_pid || '4096');
    setValue('stereoEnabled', feed.stereo_enabled !== false);
    setValue('stereoBitrate', feed.stereo_bitrate_kbps || feed.audio_bitrate_kbps || '192');
    setValue('stereoProgram', feed.stereo_program || '1');
    setValue('stereoAudioPID', mappedAudioPID('stereo', feed.stereo_audio_pid || '257'));
    setValue('stereoPmtPID', mappedProgram?.pmt_pid || feed.stereo_pmt_pid || '4096');
    setValue('syncHardReset', feed.sync_hard_reset_ms || '250');
    setValue('syncMaxAudioFrames', feed.sync_max_audio_frames_per_video || '8');
    setValue('syncSourceBuffer', feed.sync_source_buffer_ms || '240');
    setValue('syncReconnectInitial', feed.sync_reconnect_initial_ms || '500');
    setValue('syncReconnectMax', feed.sync_reconnect_max_ms || '10000');
    setValue('syncStatusInterval', feed.sync_status_interval_ms || '750');
    setValue('width', feed.width || '1920');
    setValue('height', feed.height || '1080');
    setValue('fps', feed.fps || '30000/1001');
    setValue('interlaced', Boolean(feed.interlaced));
    setValue('fieldOrder', feed.field_order || 'tff');
    setValue('standard', feed.standard || 'atsc');
    setValue('font', feed.font || 'Arial');
    setValue('fontWeight', feed.font_weight || 'regular');
    setValue('fontSize', feed.font_size || '58');
    setValue('scrollSpeed', feed.scroll_speed || '8');
    setValue('scrollRepeatMode', feed.scroll_repeat_mode || 'until_audio_end');
    setValue('afterEomRepeats', feed.after_eom_repeats || '1');
    setValue('fixedRepeats', feed.fixed_repeats || '1');
    setValue('bannerHeight', feed.banner_height || feed.ticker_height || '128');
    setValue('bannerMode', feed.banner_mode || 'auto');
    setValue('standbyMode', feed.standby_mode || 'banner');
    setValue('standbyText', feed.standby_text || 'Emergency Alert Details Channel');
    setValue('standbyFontSize', feed.standby_font_size || feed.font_size || '58');
    setValue('standbyYPercent', feed.standby_y_percent || '10');
    setValue('text', feed.text || '');
    setValue('textEnabled', Boolean(feed.text_enabled));
    setValue('textFontSize', feed.text_font_size || '58');
    setValue('clockEnabled', Boolean(feed.clock_enabled));
    setValue('clockFormat', feed.clock_format || 'Jan 02 15:04:05');
    setValue('clockX', feed.clock_x || '48');
    setValue('clockY', feed.clock_y || '48');
    setValue('clockFontSize', feed.clock_font_size || '30');
    renderOutputsEditor(Array.isArray(feed.outputs) ? feed.outputs : legacyOutputFromFeed(feed));
    updateProgramInputVisibility();
    updateAudioTopologyVisibility();
    updatePidAssignmentVisibility();
    updateEncoderControlVisibility();
    scheduleRender();
    editorDirty = false;
}

function catalogID(entry) {
    if (!entry || typeof entry !== 'object') return '';
    return String(entry.id || entry.value || entry.persistent_id || entry.device_id || '').trim();
}

function catalogLabel(entry) {
    if (!entry || typeof entry !== 'object') return '';
    return String(entry.label || entry.display_name || entry.name || entry.id || '').trim();
}

function catalogOptionTitle(entry) {
    if (!entry || typeof entry !== 'object') return '';
    const parts = [];
    if (entry.source) parts.push(`Source: ${entry.source}`);
    if (entry.element) parts.push(`GStreamer: ${entry.element}`);
    if (entry.kind) parts.push(`Kind: ${entry.kind}`);
    return parts.join('\n');
}

function populateCatalogSelect(select, entries, fallbackEntries = [], options = {}) {
    if (!select) return;
    const previous = String(select.value || '').trim();
    const list = Array.isArray(entries) && entries.length ? entries : fallbackEntries;
    select.replaceChildren();
    const seen = new Set();
    for (const entry of list) {
        const id = catalogID(entry);
        if (!id || seen.has(id)) continue;
        seen.add(id);
        const option = document.createElement('option');
        option.value = id;
        option.textContent = options.font
            ? `${catalogLabel(entry) || id} - ${entry.preview || 'The quick brown fox 0123456789'}`
            : (catalogLabel(entry) || id);
        const title = catalogOptionTitle(entry);
        if (title) option.title = title;
        if (entry.kind) option.dataset.kind = entry.kind;
        if (entry.element) option.dataset.element = entry.element;
        if (options.font) {
            option.style.fontFamily = fontCssStack(id);
        }
        select.appendChild(option);
    }
    ensureSelectOption(select, previous);
    if (previous) {
        select.value = previous;
    }
}

function populateCgenCatalogSelectors() {
    const formats = [
        { id: 'auto', label: 'Auto detect' },
        { id: 'mpegts', label: 'MPEG-TS' },
        { id: 'rtp', label: 'RTP' },
        { id: 'srt', label: 'SRT' },
        { id: 'file', label: 'File' },
        ...(cgenCatalog.formats || []),
    ];
    populateCatalogSelect(fields.programInputFormat, formats, [{ id: 'mpegts', label: 'MPEG-TS' }]);
    populateCatalogSelect(fields.hardwareDecoder, streamVideoDecoderEntries(), [
        { id: 'auto', label: 'Auto select' },
        { id: 'software', label: 'Software decoder' },
        { id: 'nvdec', label: 'NVIDIA NVDEC' },
        { id: 'quicksync', label: 'Intel QuickSync' },
        { id: 'vaapi', label: 'VAAPI' },
        { id: 'nvh264dec', label: 'H.264 / AVC - NVIDIA NVDEC (nvh264dec)', element: 'nvh264dec' },
        { id: 'avdec_h264', label: 'H.264 / AVC - libav (avdec_h264)', element: 'avdec_h264' },
        { id: 'avdec_mpeg2video', label: 'MPEG-2 Video - libav (avdec_mpeg2video)', element: 'avdec_mpeg2video' },
    ]);
    populateDeviceSelector();
    populateCatalogSelect(fields.outputFormat, [{ id: 'mpegts', label: 'MPEG-TS' }], [{ id: 'mpegts', label: 'MPEG-TS' }]);
    populateCatalogSelect(fields.vcodec, cgenCatalog.video_codecs || [], [
        { id: 'avenc_mpeg2video', label: 'MPEG-2 Video - libav (avenc_mpeg2video)', element: 'avenc_mpeg2video' },
        { id: 'x264enc', label: 'H.264 / AVC - x264 software (x264enc)', element: 'x264enc' },
    ]);
    populateCatalogSelect(fields.acodec, cgenCatalog.audio_codecs || [], [
        { id: 'avenc_ac3', label: 'AC-3 - libav (avenc_ac3)', element: 'avenc_ac3' },
        { id: 'avenc_aac', label: 'AAC - libav (avenc_aac)', element: 'avenc_aac' },
    ]);
    populateCatalogSelect(fields.font, cgenCatalog.fonts || [], [
        { id: 'Arial', label: 'Arial', preview: 'The quick brown fox 0123456789' },
        { id: 'Segoe UI', label: 'Segoe UI', preview: 'The quick brown fox 0123456789' },
    ], { font: true });
    renderFontPicker();
    updateFontPreview();
    updateProgramInputVisibility();
    updateDeinterlaceControls();
    updateAudioTopologyVisibility();
    updateEncoderControlVisibility();
}

function populateDeviceSelector() {
    const devices = Array.isArray(cgenCatalog.devices) ? cgenCatalog.devices : [];
    const backend = String(fields.deviceBackend?.value || '').toLowerCase();
    const matches = devices.filter((entry) => {
        const deviceBackend = String(entry?.backend || entry?.kind || '').toLowerCase();
        return !backend || !deviceBackend || deviceBackend === backend;
    });
    populateCatalogSelect(fields.deviceID, matches, [{ id: '', label: 'No devices discovered' }]);
    if (fields.deviceID && !fields.deviceID.options.length) {
        fields.deviceID.append(option('', 'No devices discovered'));
    }
}

function streamVideoDecoderEntries() {
    const entries = Array.isArray(cgenCatalog.video_decoders) ? cgenCatalog.video_decoders : [];
    const format = String(fields.programInputFormat?.value || 'mpegts').trim().toLowerCase();
    const standard = [
        { id: 'auto', label: 'Auto select' },
        { id: 'software', label: 'Software decoder' },
        { id: 'nvdec', label: 'NVIDIA NVDEC' },
        { id: 'quicksync', label: 'Intel QuickSync' },
        { id: 'vaapi', label: 'VAAPI' },
    ];
    const filtered = !format || ['auto', 'mpegts', 'ts', 'rtp', 'srt', 'file'].includes(format) ? entries : entries.filter((entry) => {
        const haystack = `${entry?.id || ''} ${entry?.label || ''} ${entry?.kind || ''}`.toLowerCase();
        return haystack.includes(format);
    });
    return [...standard, ...filtered];
}

function updateFontPreview() {
    if (!fontPreview || !fields.font) return;
    const family = String(fields.font.value || 'Arial').trim() || 'Arial';
    fontPreview.style.fontFamily = fontCssStack(family);
    fontPreview.textContent = 'The quick brown fox 0123456789';
    fontPreview.title = family;
    updateFontPickerSelection();
}

function updateEncoderControlVisibility() {
    const videoCodec = String(fields.vcodec?.value || '').trim().toLowerCase();
    const audioCodec = String(fields.acodec?.value || '').trim().toLowerCase();
    const supportsPreset = /(?:x264|x265|h264|h265|hevc)/.test(videoCodec);
    const supportsBFrames = /(?:x264|x265|h264|h265|hevc)/.test(videoCodec);
    const supportsProfile = /(?:x264|x265|h264|h265|hevc|aac)/.test(videoCodec);
    const videoControls = [
        [fields.videoPreset, supportsPreset],
        [fields.videoTune, supportsPreset],
        [fields.videoBFrames, supportsBFrames],
        [fields.videoProfile, supportsProfile],
        [fields.videoLevel, supportsProfile],
    ];
    for (const [field, enabled] of videoControls) {
        if (field) field.disabled = !enabled;
    }
    const audioAdvanced = /(?:aac|opus|ac3|eac3|mp2|mp3)/.test(audioCodec);
    if (fields.audioProfile) fields.audioProfile.disabled = !audioAdvanced;
    if (fields.audioLevel) fields.audioLevel.disabled = !audioAdvanced;
}

function fontCssStack(family) {
    const cleaned = String(family || 'Arial').trim().replace(/\\/g, '\\\\').replace(/"/g, '\\"') || 'Arial';
    const base = cleaned.replace(/\s+(regular|bold|italic|black|medium|semibold|semi-bold|light|thin|cond|condensed|narrow|roman|book|heavy|variable|[1-9][0-9]{1,2})+$/i, '').trim();
    const fallback = base && base !== cleaned ? `, "${base.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"` : '';
    return `"${cleaned}"${fallback}, Arial, sans-serif`;
}

function registerManagedFontFaces() {
    if (!('FontFace' in window) || !document.fonts) return;
    const fonts = Array.isArray(cgenCatalog.fonts) ? cgenCatalog.fonts : [];
    for (const entry of fonts) {
        const family = catalogID(entry);
        const source = String(entry?.url || '').trim();
        if (!family || !source) continue;
        const key = `${family}\n${source}`;
        if (managedFontFaces.has(key)) continue;
        const face = new FontFace(family, `url(${JSON.stringify(source)})`);
        managedFontFaces.set(key, face);
        face.load().then((loaded) => {
            document.fonts.add(loaded);
            updateFontPreview();
            updateFontPickerSelection();
        }).catch(() => {});
    }
}

function fontPickerEntries() {
    const source = Array.isArray(cgenCatalog.fonts) && cgenCatalog.fonts.length
        ? cgenCatalog.fonts
        : [
            { id: 'Arial', label: 'Arial', preview: 'The quick brown fox 0123456789' },
            { id: 'Segoe UI', label: 'Segoe UI', preview: 'The quick brown fox 0123456789' },
        ];
    const seen = new Set();
    const entries = [];
    for (const entry of source) {
        const id = catalogID(entry);
        if (!id || seen.has(id.toLowerCase())) continue;
        seen.add(id.toLowerCase());
        entries.push({
            id,
            label: catalogLabel(entry) || id,
            preview: entry?.preview || 'The quick brown fox 0123456789',
        });
    }
    const current = String(fields.font?.value || '').trim();
    if (current && !seen.has(current.toLowerCase())) {
        entries.unshift({ id: current, label: current, preview: 'The quick brown fox 0123456789' });
    }
    return entries;
}

function renderFontPicker() {
    if (!fontMenu) return;
    fontMenu.replaceChildren();
    for (const entry of fontPickerEntries()) {
        const option = document.createElement('button');
        option.type = 'button';
        option.className = 'cgen-font-option';
        option.dataset.value = entry.id;
        option.setAttribute('role', 'option');
        option.style.fontFamily = fontCssStack(entry.id);
        const name = document.createElement('span');
        name.className = 'cgen-font-option-name';
        name.textContent = entry.label;
        const preview = document.createElement('span');
        preview.className = 'cgen-font-option-preview';
        preview.textContent = `- ${entry.preview}`;
        option.append(name, preview);
        option.addEventListener('click', () => selectFontFamily(entry.id));
        fontMenu.appendChild(option);
    }
    updateFontPickerSelection();
}

function updateFontPickerSelection() {
    const family = String(fields.font?.value || 'Arial').trim() || 'Arial';
    const entry = fontPickerEntries().find((item) => item.id === family);
    if (fontPickerLabel) {
        fontPickerLabel.textContent = `${entry?.label || family} - ${entry?.preview || 'The quick brown fox 0123456789'}`;
        fontPickerLabel.style.fontFamily = fontCssStack(family);
    }
    if (fontPicker) {
        fontPicker.title = family;
    }
    if (fontMenu) {
        fontMenu.querySelectorAll('.cgen-font-option').forEach((option) => {
            option.setAttribute('aria-selected', option.dataset.value === family ? 'true' : 'false');
        });
    }
}

function selectFontFamily(family) {
    if (!fields.font) return;
    ensureSelectOption(fields.font, family);
    fields.font.value = family;
    editorDirty = true;
    setFontPickerOpen(false);
    updateFontPreview();
    scheduleRender();
}

function setFontPickerOpen(open) {
    if (!fontPicker || !fontMenu) return;
    fontMenu.hidden = !open;
    fontPicker.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (open) {
        updateFontPickerSelection();
        const selectedOption = fontMenu.querySelector('.cgen-font-option[aria-selected="true"]') || fontMenu.querySelector('.cgen-font-option');
        selectedOption?.scrollIntoView({ block: 'nearest' });
    }
}

function toggleFontPicker() {
    setFontPickerOpen(fontMenu?.hidden !== false);
}

function upsertEditor() {
    const edited = readEditor();
    const index = feeds.findIndex((feed) => feed.id === selectedID);
    if (index >= 0) {
        edited.runtime = feeds[index].runtime || {};
        feeds[index] = edited;
    } else {
        feeds.push(edited);
    }
    selectedID = edited.id;
}

function renderInstances() {
    instanceSelect.innerHTML = feeds.map((feed) => `<option value="${escapeHtml(feed.id)}">${escapeHtml(feed.name || feed.id)}</option>`).join('');
    if (selectedID && feeds.some((feed) => feed.id === selectedID)) {
        instanceSelect.value = selectedID;
    } else if (feeds[0]) {
        selectedID = feeds[0].id;
        instanceSelect.value = selectedID;
    }
    setText(countMetric, String(feeds.length));
    setText(modeMetric, selected()?.mode || 'release');
}

function renderMeta() {
    const feed = readEditor();
    const runtime = selected()?.runtime || {};
    const inputHealth = runtime.input_health && typeof runtime.input_health === 'object' ? runtime.input_health : {};
    const diagnostics = runtime.pipeline_diagnostics && typeof runtime.pipeline_diagnostics === 'object' ? runtime.pipeline_diagnostics : {};
    setText(metaProgramInput, programInputSummary(feed));
    const standbyMute = feed.mute_standby_routine === false ? 'standby routine live' : 'standby routine muted';
    const standbyAudio = feed.audio_idle === 'routine' ? 'feed routine standby' : 'program input standby';
    setText(metaPriority, `${feed.priority_feed_id || feed.id || '-'} / ${feed.audio_source || 'priority'} / ${standbyAudio} / ${standbyMute}`);
    const configuredOutputs = Array.isArray(feed.outputs) ? feed.outputs.filter((output) => output.enabled !== false) : [];
    setText(metaOutput, configuredOutputs.length
        ? `${configuredOutputs.length} isolated destination${configuredOutputs.length === 1 ? '' : 's'}`
        : redactEndpointForDisplay(feed.program_output_url));
    if (metaRuntime) {
        const videoLive = runtime.input_video_connected === true || inputHealth.video_connected === true;
        const audioLive = runtime.input_audio_connected === true || inputHealth.audio_connected === true;
        const videoTimedOut = inputHealth.video_timed_out === true;
        const audioTimedOut = inputHealth.audio_timed_out === true;
        const connected = `${formatStreamHealth('video', videoLive, videoTimedOut, inputHealth.last_video_frame_age_ms || inputHealth.last_program_frame_age_ms)}, ${formatStreamHealth('audio', audioLive, audioTimedOut, inputHealth.last_audio_frame_age_ms)}`;
        const output = runtime.output_active === true ? 'output active' : 'output idle';
        const backend = runtime.media_backend || 'cgen';
        const gstState = runtime.gst_state ? `, ${runtime.gst_state}` : '';
        const ancillaryWarnings = ancillaryDegradationWarnings(runtime.ancillary_capabilities);
        const ancillaryDegraded = ancillaryWarnings.length;
        const ancillary = ancillaryDegraded > 0 ? `, ${ancillaryDegraded} ancillary request${ancillaryDegraded === 1 ? '' : 's'} unavailable` : '';
        setText(metaRuntime, `${backend}: ${connected}, ${output}${gstState}${ancillary}`);
        metaRuntime.title = ancillaryWarnings.join('\n');
    }
    if (metaDrift) {
        setText(metaDrift, formatPipelineDiagnostics(diagnostics));
    }
    if (metaVisual) {
        const visual = runtime.visual_lifecycle || runtime.visual_mode || feed.mode || 'release';
        const video = runtime.video_selector || 'video?';
        const audio = runtime.audio_selector || 'audio?';
        const sunny = sunnyAvailableForFeed(feed) ? ` / Sunny ${feed.sunny_cat ? 'unleashed' : 'banished'}` : '';
        setText(metaVisual, `${visual} / ${video} video / ${audio} audio${sunny}`);
    }
    updateSunnyVisibility(feed, runtime);
    updatePreviewStream(feed);
}

function ancillaryDegradationWarnings(capabilities) {
    const outputs = Array.isArray(capabilities?.outputs) ? capabilities.outputs : [];
    const warnings = [];
    for (const output of outputs) {
        const outputID = String(output?.output_id || 'output');
        const captionPolicies = [output?.eia608?.captions, output?.eia708?.captions];
        const caption = captionPolicies.find((item) => item?.requested === 'pass' && item?.effective !== 'pass');
        if (caption) warnings.push(`${outputID} captions: ${caption.warning || 'requested passthrough is unavailable'}`);
        const sharedPolicy = output?.eia708 || output?.eia608;
        for (const kind of ['scte35', 'scte104']) {
            const item = sharedPolicy?.[kind];
            if (item?.requested === 'pass' && item?.effective !== 'pass') {
                warnings.push(`${outputID} ${kind.toUpperCase()}: ${item.warning || 'requested passthrough is unavailable'}`);
            }
        }
    }
    return warnings;
}

function programInputSummary(feed) {
    switch (feed.program_input_type) {
        case 'device':
            return `${feed.device_backend || 'device'} / ${feed.device_id || 'not selected'}`;
        case 'dummy':
            return `dummy ${feed.dummy_width || 720}x${feed.dummy_height || 480} ${feed.dummy_fps || '30000/1001'} ${feed.dummy_scan_mode || 'progressive'}`;
        default:
            return redactEndpointForDisplay(feed.program_input_url);
    }
}

function redactEndpointForDisplay(value) {
    const raw = String(value || '').trim();
    if (!raw) return '-';
    if (/\$\{|\$\(|%[A-Za-z_][A-Za-z0-9_]*%/.test(raw)) return '[environment reference]';
    try {
        const parsed = new URL(raw, window.location.origin);
        const explicitScheme = /^[a-z][a-z0-9+.-]*:/i.test(raw);
        parsed.username = '';
        parsed.password = '';
        const hadOptions = parsed.search.length > 1;
        parsed.search = '';
        const text = explicitScheme ? parsed.toString() : `${parsed.pathname}${parsed.search}${parsed.hash}`;
        const summary = `${text}${hadOptions ? ' [options hidden]' : ''}`;
        return summary.length > 160 ? `${summary.slice(0, 157)}...` : summary;
    } catch {
        return '[configured endpoint]';
    }
}

function formatStreamHealth(label, live, timedOut, ageValue) {
    const state = live ? 'live' : timedOut ? 'timeout' : 'waiting';
    return `${label} ${state}${formatRuntimeAge(ageValue)}`;
}

function formatRuntimeAge(value) {
    const age = Number(value);
    if (!Number.isFinite(age)) return '';
    if (age < 1000) return ` ${Math.round(age)} ms`;
    return ` ${(age / 1000).toFixed(1)} s`;
}

function formatPipelineDiagnostics(diagnostics) {
    const parts = [];
    const warnings = Number(diagnostics.warning_count);
    const qos = Number(diagnostics.qos_count);
    const latency = Number(diagnostics.latency_recalculation_count);
    if (Number.isFinite(latency)) parts.push(`${latency} latency recalcs`);
    if (Number.isFinite(qos)) parts.push(`${qos} QoS`);
    if (Number.isFinite(warnings)) parts.push(`${warnings} warnings`);
    if (diagnostics.last_latency_error) parts.push(`latency: ${diagnostics.last_latency_error}`);
    if (diagnostics.last_qos && typeof diagnostics.last_qos === 'object') {
        const q = diagnostics.last_qos;
        const jitter = Number(q.jitter_ns);
        const proportion = Number(q.proportion);
        const qosParts = [];
        if (Number.isFinite(jitter)) qosParts.push(`${(jitter / 1000000).toFixed(1)} ms jitter`);
        if (Number.isFinite(proportion)) qosParts.push(`${proportion.toFixed(3)}x`);
        if (q.dropped) qosParts.push(`dropped ${q.dropped}`);
        if (qosParts.length) parts.push(`last QoS ${qosParts.join(' ')}`);
    }
    if (diagnostics.last_warning) parts.push(`warn: ${diagnostics.last_warning}`);
    return parts.join(', ') || '-';
}

function drawSmpte(ctx, width, height) {
    const top = ['#c0c0c0', '#c0c000', '#00c0c0', '#00c000', '#c000c0', '#c00000', '#0000c0'];
    const topHeight = Math.floor(height * 0.67);
    top.forEach((color, index) => {
        ctx.fillStyle = color;
        ctx.fillRect(Math.floor(index * width / top.length), 0, Math.ceil(width / top.length), topHeight);
    });
    const mid = ['#0000c0', '#131313', '#c000c0', '#131313', '#00c0c0', '#131313', '#c0c0c0'];
    const midHeight = Math.floor(height * 0.12);
    mid.forEach((color, index) => {
        ctx.fillStyle = color;
        ctx.fillRect(Math.floor(index * width / mid.length), topHeight, Math.ceil(width / mid.length), midHeight);
    });
    const bottomY = topHeight + midHeight;
    const bottom = ['#00214c', '#ffffff', '#32006a', '#131313', '#090909', '#1d1d1d', '#090909', '#131313'];
    bottom.forEach((color, index) => {
        ctx.fillStyle = color;
        ctx.fillRect(Math.floor(index * width / bottom.length), bottomY, Math.ceil(width / bottom.length), height - bottomY);
    });
}

function clampNumber(value, min, max, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.min(max, Math.max(min, n));
}

function drawStandby(ctx, width, height, feed) {
    const gradient = ctx.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, '#111827');
    gradient.addColorStop(0.5, '#000000');
    gradient.addColorStop(1, '#111827');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, width, height);

    const outputHeight = clampNumber(feed.height, 1, 4320, 1080);
    const sy = height / outputHeight;
    const fontSize = Math.max(8, clampNumber(feed.standby_font_size || feed.font_size, 8, 220, 58) * sy);
    const yPercent = clampNumber(feed.standby_y_percent, 0, 100, 10);
    const text = feed.standby_text || 'Emergency Alert Details Channel';
    const font = feed.font || 'Arial';
    ctx.font = `${canvasFontWeight(feed.font_weight, 'regular')} ${fontSize}px ${font}, Arial, sans-serif`;
    ctx.textBaseline = 'top';
    const x = Math.max(0, (width - ctx.measureText(text).width) / 2);
    const y = height * (yPercent / 100);
    ctx.fillStyle = 'rgba(0, 0, 0, 0.85)';
    ctx.fillText(text, x + 2, y + 3);
    ctx.fillStyle = '#ffffff';
    ctx.fillText(text, x, y);
}

function drawProgramPlaceholder(ctx, width, height, feed, runtime) {
    const live = runtime.input_connected === true && runtime.no_signal !== true;
    const gradient = ctx.createLinearGradient(0, 0, width, height);
    gradient.addColorStop(0, live ? '#12324b' : '#111827');
    gradient.addColorStop(0.5, live ? '#1b5f7a' : '#050505');
    gradient.addColorStop(1, live ? '#14301f' : '#111827');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, width, height);
    ctx.strokeStyle = 'rgba(255,255,255,.12)';
    ctx.lineWidth = 1;
    for (let y = 0; y < height; y += 18) {
        ctx.beginPath();
        ctx.moveTo(0, y + 0.5);
        ctx.lineTo(width, y + 0.5);
        ctx.stroke();
    }
    ctx.fillStyle = 'rgba(0,0,0,.42)';
    ctx.fillRect(14, 14, Math.min(width - 28, 420), 74);
    ctx.fillStyle = '#ffffff';
    ctx.font = `700 ${Math.max(13, width / 38)}px Arial, sans-serif`;
    ctx.textBaseline = 'top';
    ctx.fillText(live ? 'PROGRAM INPUT LIVE' : 'NO PROGRAM INPUT', 28, 26);
    ctx.font = `500 ${Math.max(10, width / 58)}px Arial, sans-serif`;
    const label = `${feed.width || 1920}x${feed.height || 1080} ${feed.interlaced ? 'interlaced' : 'progressive'}  ${feed.vcodec || 'avenc_mpeg2video'} / ${feed.acodec || 'avenc_ac3'}`;
    ctx.fillText(label, 28, 58);
}

function drawCompositorTicker(ctx, width, height, feed, runtime, text) {
    const outputWidth = clampNumber(feed.width, 1, 7680, 1920);
    const outputHeight = clampNumber(feed.height, 1, 4320, 1080);
    const sx = width / outputWidth;
    const sy = height / outputHeight;
    const y = clampNumber(runtime.ticker_y, 0, outputHeight, Math.round(outputHeight * 0.08)) * sy;
    const h = clampNumber(runtime.ticker_height ?? feed.banner_height, 24, outputHeight, 128) * sy;
    const gradientStops = Array.isArray(runtime.ticker_gradient) && runtime.ticker_gradient.length >= 3
        ? runtime.ticker_gradient
        : ['#111827', runtime.ticker_color || '#019310', '#111827'];
    const gradient = ctx.createLinearGradient(0, y, 0, y + h);
    gradient.addColorStop(0, gradientStops[0]);
    gradient.addColorStop(0.5, gradientStops[1]);
    gradient.addColorStop(1, gradientStops[2]);
    ctx.fillStyle = gradient;
    ctx.fillRect(0, y, width, h);
    const fontSize = Math.max(10, clampNumber(runtime.font_size ?? feed.font_size, 8, 220, 58) * sy);
    const font = runtime.font || feed.font || 'Arial';
    ctx.font = `${canvasFontWeight(runtime.font_weight || feed.font_weight, 'regular')} ${fontSize}px ${font}, Arial, sans-serif`;
    ctx.textBaseline = 'middle';
    const textY = y + h / 2;
    ctx.fillStyle = 'rgba(0,0,0,.9)';
    ctx.fillText(text, width * 0.03 + 3, textY + 4);
    ctx.fillStyle = '#ffffff';
    ctx.fillText(text, width * 0.03, textY);
}

function drawSunnyPreview(ctx, width, height) {
    const w = Math.min(width * 0.22, 120);
    const h = w * 1.25;
    const x = width - w - 18;
    const y = height - h - 18;
    const gradient = ctx.createLinearGradient(x, y, x + w, y + h);
    gradient.addColorStop(0, '#ffd98a');
    gradient.addColorStop(0.55, '#f28c28');
    gradient.addColorStop(1, '#5b2f13');
    ctx.fillStyle = gradient;
    ctx.fillRect(x, y, w, h);
    ctx.fillStyle = '#1b1009';
    ctx.fillRect(x + w * 0.18, y + h * 0.18, w * 0.16, h * 0.08);
    ctx.fillRect(x + w * 0.66, y + h * 0.18, w * 0.16, h * 0.08);
    ctx.fillStyle = '#ffffff';
    ctx.font = `700 ${Math.max(10, w / 9)}px Arial, sans-serif`;
    ctx.textBaseline = 'bottom';
    ctx.fillText('SUNNY', x + w * 0.08, y + h - 8);
}

function renderPreview() {
    if (!preview || (previewStream && !previewStream.hidden)) return;
    const ctx = preview.getContext('2d');
    const width = preview.width;
    const height = preview.height;
    const feed = readEditor();
    const runtime = selected()?.runtime || {};
    const liveNoSignal = runtime.no_signal === true || runtime.input_connected === false || runtime.visual_lifecycle === 'standby';
    const visualMode = String(runtime.visual_mode || '').toLowerCase();
    const activeText = String(runtime.overlay_text || '').trim();
    const isSmpte = feed.smpte_bars || visualMode === 'smpte' || (!runtime.input_connected && feed.standby_mode === 'smpte');
    if (isSmpte) {
        drawSmpte(ctx, width, height);
    } else if (liveNoSignal && activeText) {
        ctx.fillStyle = '#000000';
        ctx.fillRect(0, 0, width, height);
    } else if (liveNoSignal && !activeText && feed.standby_mode === 'black') {
        ctx.fillStyle = '#000000';
        ctx.fillRect(0, 0, width, height);
    } else if (liveNoSignal && !activeText) {
        drawStandby(ctx, width, height, feed);
    } else {
        drawProgramPlaceholder(ctx, width, height, feed, runtime);
    }
    if (activeText) {
        drawCompositorTicker(ctx, width, height, feed, runtime, activeText);
    }
    if (feed.sunny_cat && sunnyAvailableForFeed(feed)) {
        drawSunnyPreview(ctx, width, height);
    }
    ctx.fillStyle = '#ffffff';
    const sx = width / Number(feed.width || 1280);
    const sy = height / Number(feed.height || 720);
    ctx.font = `${Math.max(8, Number(feed.text_font_size || feed.font_size || 58) * sy)}px sans-serif`;
    if (feed.text_enabled && feed.text && !activeText) {
        ctx.fillText(feed.text.slice(0, 80), 48 * sx, 96 * sy);
    }
    if (feed.clock_enabled) {
        ctx.fillStyle = '#ffffff';
        ctx.font = `${Math.max(8, Number(feed.clock_font_size || 30) * sy)}px monospace`;
        ctx.fillText(new Date().toLocaleString(), Number(feed.clock_x || 48) * sx, Number(feed.clock_y || 48) * sy);
    }
}

function updatePreviewStream(feed = selected()) {
    if (!previewStream || !feed?.id) return;
    if (previewStreamFeedID === feed.id && previewStream.src) return;
    previewStreamFeedID = feed.id;
    previewStream.hidden = true;
    if (preview) preview.hidden = false;
    const url = `/api/v1/cgen/preview?feed=${encodeURIComponent(feed.id)}&t=${Date.now()}`;
    previewStream.src = url;
}

function sunnyAvailableForFeed(feed) {
    const runtime = feed?.runtime || {};
    if (runtime.sunny_cat_available === true) return true;
    const compositor = runtime.compositor || runtime.text_overlay || {};
    if (compositor.sunny_cat_available === true) return true;
    const renderer = runtime.graphics_renderer || {};
    if (renderer.sunny_cat_available === true) return true;
    if (Array.isArray(renderer.renditions)) {
        return renderer.renditions.some((rendition) => rendition && rendition.sunny_cat_available === true);
    }
    return false;
}

function updateSunnyVisibility(feed = selected(), runtime = feed?.runtime || {}) {
    const available = sunnyAvailableForFeed({ ...feed, runtime });
    if (sunnyField) sunnyField.hidden = !available;
    if (sunnyButton) {
        sunnyButton.hidden = !available;
        sunnyButton.textContent = feed?.sunny_cat ? 'Banish Loathed Creature' : 'Unleash Sunny, the Cat';
    }
}

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[char]));
}

function sceneByID(id) {
    return scenes.find((scene) => scene.id === id) || null;
}

function protectedAlertScene(id) {
    return id !== 'Program_Passthrough' && id !== 'Standby';
}

function populateAlertSceneOptions() {
    if (!fields.alertScene) return;
    const current = String(fields.alertScene.value || selected()?.alert_scene_id || 'Standard_Crawl');
    const available = scenes.filter((scene) => protectedAlertScene(scene.id));
    const defaults = [
        { id: 'Standard_Crawl', name: 'Standard_Crawl' },
        { id: 'Fullscreen_Takeover', name: 'Fullscreen_Takeover' },
    ];
    const merged = [...defaults, ...available];
    fields.alertScene.replaceChildren();
    const seen = new Set();
    for (const scene of merged) {
        if (!scene.id || seen.has(scene.id)) continue;
        seen.add(scene.id);
        fields.alertScene.append(option(scene.id, scene.name || scene.id));
    }
    ensureSelectOption(fields.alertScene, protectedAlertScene(current) ? current : 'Standard_Crawl');
    fields.alertScene.value = protectedAlertScene(current) ? current : 'Standard_Crawl';
}

function renderSceneCatalog(selectedSceneID = '') {
    if (!sceneSelect) return;
    const selectedValue = selectedSceneID || activeScene?.id || sceneSelect.value;
    sceneSelect.replaceChildren();
    if (!scenes.length) {
        sceneSelect.append(option('', 'No managed scenes found'));
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
    sceneCollectionRevision = String(payload.revision || '');
    const nextID = selectID || activeScene?.id || scenes[0]?.id || '';
    renderSceneCatalog(nextID);
    if (nextID && sceneByID(nextID)) {
        await loadScene(nextID);
    } else {
        activeScene = null;
        sceneDirty = false;
        if (sceneXML) sceneXML.value = '';
        renderSceneState('No scene document selected.');
    }
    if (announce) setStatus(`Scene catalog refreshed: ${scenes.length} documents.`, 'ok');
}

function newSceneXML(id) {
    return `<?xml version="1.0" encoding="UTF-8"?>\n<scene schema_version="1" id="${id}" name="${id}">\n  <node id="root" name="Root" enabled="true">\n    <transform x="0" y="0" width="0" height="0" z_index="0" opacity="1" clip_children="false">\n      <anchors left="0" top="0" right="1" bottom="1"/>\n    </transform>\n    <group/>\n  </node>\n</scene>\n`;
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
    renderSceneState('New scene is not saved yet. Edit the id and name in XML if needed.');
}

async function saveScene() {
    if (!activeScene || activeScene.locked) return;
    const xml = String(sceneXML?.value || '');
    if (!xml.trim()) throw new Error('Scene XML is required.');
    sceneSaveButton.disabled = true;
    try {
        const request = {
            expected_revision: activeScene.revision || '',
            xml,
        };
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
    feeds = Array.isArray(payload.feeds) ? payload.feeds : [];
    if (pathLabel) pathLabel.textContent = payload.path || 'managed/configs/cgen.xml';
    globalEnabled.checked = cgenEnabled;
    if (!feeds.length) {
        feeds.push(defaultFeed());
    }
    renderInstances();
    writeEditor(selected());
    updatePreviewStream(selected());
    setStatus('CGEN config loaded.', 'ok');
}

async function loadCgenCatalog({ announce = false } = {}) {
    const payload = await panelClient.command('cgen.catalog', {}, 15000);
    cgenCatalog = {
        formats: Array.isArray(payload.formats) ? payload.formats : [],
        video_codecs: Array.isArray(payload.video_codecs) ? payload.video_codecs : [],
        audio_codecs: Array.isArray(payload.audio_codecs) ? payload.audio_codecs : [],
        video_decoders: Array.isArray(payload.video_decoders) ? payload.video_decoders : [],
        deinterlacers: Array.isArray(payload.deinterlacers) ? payload.deinterlacers : [],
        capabilities: payload.capabilities && typeof payload.capabilities === 'object' ? payload.capabilities : {},
        devices: Array.isArray(payload.devices) ? payload.devices : (Array.isArray(payload.input_devices) ? payload.input_devices : []),
        fonts: Array.isArray(payload.fonts) ? payload.fonts : [],
    };
    applyCatalogCapabilities();
    registerManagedFontFaces();
    populateCgenCatalogSelectors();
    if (announce) {
        const fontCount = cgenCatalog.fonts.length;
        const videoCount = cgenCatalog.video_codecs.length;
        const audioCount = cgenCatalog.audio_codecs.length;
        setStatus(`Catalog refreshed: ${videoCount} video codecs, ${audioCount} audio codecs, ${fontCount} fonts.`, 'ok');
    }
}

function preserveNativeAudioAvailable() {
    return cgenCatalog?.capabilities?.audio_topologies?.preserve_native_tracks === true;
}

function applyCatalogCapabilities() {
    const option = fields.audioTopology?.querySelector('option[value="preserve_native_tracks"]');
    if (!option) return;
    const available = preserveNativeAudioAvailable();
    option.disabled = !available;
    option.textContent = available
        ? 'Preserve native tracks'
        : 'Preserve native tracks (backend unavailable)';
}

async function refreshRuntime() {
    const payload = await panelClient.command('cgen.get', {}, 10000);
    if (!editorDirty) {
        cgenRevision = String(payload.revision || payload.config_revision || payload.hash || cgenRevision);
    }
    const latest = Array.isArray(payload.feeds) ? payload.feeds : [];
    for (const next of latest) {
        const index = feeds.findIndex((feed) => feed.id === next.id);
        if (index >= 0) {
            feeds[index].runtime = next.runtime || {};
            if (!editorDirty && next.id === selectedID) {
                feeds[index] = next;
            }
        }
    }
    if (!editorDirty) {
        renderInstances();
    }
    scheduleRender();
    updatePreviewStream(selected());
}

async function saveCgen() {
    validatePipelineEditor(readEditor());
    upsertEditor();
    const payload = await panelClient.command('cgen.save', {
        enabled: globalEnabled.checked,
        feeds,
        expected_revision: cgenRevision,
    }, 12000);
    cgenEnabled = payload.enabled !== false;
    cgenRevision = String(payload.revision || payload.config_revision || payload.hash || cgenRevision);
    feeds = Array.isArray(payload.feeds) ? payload.feeds : feeds;
    globalEnabled.checked = cgenEnabled;
    renderInstances();
    writeEditor(selected());
    setStatus('CGEN XML saved.', 'ok');
}

function defaultFeed() {
    return {
        id: 'CFSP-CAP',
        name: 'CFSP/CAP CGEN',
        enabled: true,
        mode: 'release',
        program_input_type: 'uri_or_file',
        program_input_url: 'udp://239.0.0.1:9000?fifo_size=2000000&overrun_nonfatal=1&reuse=1&buffer_size=1048576',
        program_input_format: 'mpegts',
        hardware_decoder_enabled: false,
        hardware_decoder: '',
        decoder_preference: 'auto',
        deinterlace_algorithm: 'yadif',
        deinterlace_backend: 'software',
        deinterlace_cadence: 'field',
        deinterlace_parity: 'auto',
        device_backend: 'v4l2',
        device_id: '',
        dummy_width: '720',
        dummy_height: '480',
        dummy_fps: '30000/1001',
        dummy_scan_mode: 'progressive',
        dummy_background: '#000000FF',
        priority_feed_id: 'CFSP-CAP',
        alert_feed_id: 'CFSP-CAP',
        audio_source: 'priority',
        audio_idle: 'source',
        mute_standby_routine: true,
        ancillary_captions: 'drop',
        ancillary_scte35: 'drop',
        ancillary_scte104: 'drop',
        audio_topology: 'force_layout',
        audio_force_layout: 'stereo',
        audio_idle_program_gain_db: '0',
        audio_alert_program_gain_db: 'muted',
        audio_alert_gain_db: '0',
        audio_transition_ms: '20',
        compositor_engine: 'scene_v2',
        alert_scene_id: 'Standard_Crawl',
        pid_assignment: 'auto',
        generated_alert_cues: true,
        program_output_url: 'udp://239.0.0.2:9001?pkt_size=1316&buffer_size=1048576&reuse=1',
        program_output_format: 'mpegts',
        vcodec: 'avenc_mpeg2video',
        acodec: 'avenc_ac3',
        video_bitrate_kbps: '12000',
        audio_bitrate_kbps: '192',
        video_encoder_codec: 'avenc_mpeg2video',
        video_encoder_bitrate_kbps: '12000',
        video_gop: '15',
        video_bframes: '0',
        video_preset: '',
        video_tune: '',
        video_profile: '',
        video_level: '',
        audio_encoder_codec: 'avenc_ac3',
        audio_encoder_bitrate_kbps: '192',
        audio_profile: '',
        audio_level: '',
        service_name: 'Haze CGEN',
        provider_name: 'Haze',
        service_id: '1',
        transport_stream_id: '1',
        hd_enabled: 'auto',
        hd_bitrate_kbps: '12000',
        hd_program: '1',
        hd_video_pid: '256',
        hd_pmt_pid: '4096',
        p720_enabled: false,
        p720_bitrate_kbps: '8000',
        p720_program: '2',
        p720_video_pid: '288',
        p720_pmt_pid: '4097',
        sd_enabled: false,
        sd_bitrate_kbps: '5000',
        sd_program: '3',
        sd_video_pid: '320',
        sd_pmt_pid: '4098',
        surround_enabled: true,
        surround_bitrate_kbps: '384',
        surround_program: '1',
        surround_audio_pid: '258',
        surround_pmt_pid: '4096',
        stereo_enabled: true,
        stereo_bitrate_kbps: '192',
        stereo_program: '1',
        stereo_audio_pid: '257',
        stereo_pmt_pid: '4096',
        sync_hard_reset_ms: '250',
        sync_max_audio_frames_per_video: '8',
        sync_source_buffer_ms: '240',
        sync_reconnect_initial_ms: '500',
        sync_reconnect_max_ms: '10000',
        sync_status_interval_ms: '750',
        width: '1920',
        height: '1080',
        fps: '30000/1001',
        interlaced: true,
        field_order: 'tff',
        standard: 'atsc',
        banner_mode: 'auto',
        scroll_speed: '8',
        scroll_repeat_mode: 'until_audio_end',
        after_eom_repeats: '1',
        fixed_repeats: '1',
        banner_height: '128',
        standby_mode: 'banner',
        standby_text: 'Emergency Alert Details Channel',
        standby_font_size: '58',
        standby_y_percent: '10',
        font: 'Arial',
        font_weight: 'regular',
        font_size: '58',
        text_font_size: '58',
        clock_x: '48',
        clock_y: '48',
        clock_font_size: '30',
        sunny_cat: false,
        outputs: [{
            id: 'main',
            enabled: true,
            destination: 'mpeg_ts_udp',
            url: 'udp://239.0.0.2:9001?pkt_size=1316&buffer_size=1048576&reuse=1',
            video_codec: 'mpeg2',
            audio_codec: 'ac3',
            video_bitrate_kbps: '12000',
            audio_bitrate_kbps: '192',
            sample_rate: '48000',
            gop_frames: '15',
            rate_control: 'cbr',
        }],
    };
}

function canvasFontWeight(value, fallback = 'regular') {
    switch (String(value || fallback).trim().toLowerCase()) {
        case 'thin':
        case '100':
            return '100';
        case 'extra-light':
        case 'extralight':
        case '200':
            return '200';
        case 'light':
        case '300':
            return '300';
        case 'medium':
        case '500':
            return '500';
        case 'semibold':
        case 'semi-bold':
        case '600':
            return '600';
        case 'bold':
        case '700':
            return '700';
        case 'extra-bold':
        case 'extrabold':
        case '800':
            return '800';
        case 'black':
        case '900':
            return '900';
        default:
            return '400';
    }
}

async function runAction(action, extra = {}) {
    upsertEditor();
    const current = readEditor();
    const payload = await panelClient.command('cgen.action', { ...current, feed_id: selectedID, action, ...extra }, 10000);
    feeds = Array.isArray(payload.feeds) ? payload.feeds : feeds;
    renderInstances();
    writeEditor(feeds.find((feed) => feed.id === selectedID) || feeds[0]);
    setStatus(`CGEN action applied: ${action.replaceAll('_', ' ')}.`, 'ok');
}

function addInstance() {
    upsertEditor();
    const next = defaultFeed();
    next.id = sanitizeID(`cgen-${feeds.length + 1}`);
    next.name = `CGEN ${feeds.length + 1}`;
    next.priority_feed_id = next.id;
    next.alert_feed_id = next.id;
    feeds.push(next);
    selectedID = next.id;
    renderInstances();
    writeEditor(next);
    setStatus('New CGEN instance ready to edit.', 'pending');
}

export function initCgenView() {
    if (bound) return;
    bound = true;
    bindCgenTabs();
    renderSceneState('Scene catalog not loaded.');
    addButton.addEventListener('click', addInstance);
    saveButton.addEventListener('click', () => saveCgen().catch((error) => setStatus(error.message || 'Unable to save CGEN config.', 'err')));
    instanceSelect.addEventListener('change', () => {
        upsertEditor();
        selectedID = instanceSelect.value;
        writeEditor(selected());
    });
    Object.values(fields).forEach((field) => {
        field?.addEventListener('input', () => {
            editorDirty = true;
            scheduleRender();
            if (field === fields.font) updateFontPreview();
            if (field === fields.vcodec || field === fields.acodec) updateEncoderControlVisibility();
            if (field === fields.programInputType || field === fields.hardwareDecoderEnabled) updateProgramInputVisibility();
            if (field === fields.deinterlaceAlgorithm) updateDeinterlaceControls({ selectFallback: true });
            if (field === fields.deinterlaceBackend) updateDeinterlaceControls();
            if (field === fields.audioTopology) updateAudioTopologyVisibility();
            if (field === fields.pidAssignment) updatePidAssignmentVisibility();
            if (field === fields.deviceBackend) populateDeviceSelector();
            if (field === fields.programInputFormat) {
                populateCatalogSelect(fields.hardwareDecoder, streamVideoDecoderEntries(), [
                    { id: 'nvh264dec', label: 'H.264 / AVC - NVIDIA NVDEC (nvh264dec)', element: 'nvh264dec' },
                    { id: 'avdec_h264', label: 'H.264 / AVC - libav (avdec_h264)', element: 'avdec_h264' },
                    { id: 'avdec_mpeg2video', label: 'MPEG-2 Video - libav (avdec_mpeg2video)', element: 'avdec_mpeg2video' },
                ]);
                updateProgramInputVisibility();
            }
        });
        field?.addEventListener('change', () => {
            editorDirty = true;
            scheduleRender();
            if (field === fields.font) updateFontPreview();
            if (field === fields.vcodec || field === fields.acodec) updateEncoderControlVisibility();
            if (field === fields.programInputType || field === fields.hardwareDecoderEnabled) updateProgramInputVisibility();
            if (field === fields.deinterlaceAlgorithm) updateDeinterlaceControls({ selectFallback: true });
            if (field === fields.deinterlaceBackend) updateDeinterlaceControls();
            if (field === fields.audioTopology) updateAudioTopologyVisibility();
            if (field === fields.pidAssignment) updatePidAssignmentVisibility();
            if (field === fields.deviceBackend) populateDeviceSelector();
            if (field === fields.programInputFormat) {
                populateCatalogSelect(fields.hardwareDecoder, streamVideoDecoderEntries(), [
                    { id: 'nvh264dec', label: 'H.264 / AVC - NVIDIA NVDEC (nvh264dec)', element: 'nvh264dec' },
                    { id: 'avdec_h264', label: 'H.264 / AVC - libav (avdec_h264)', element: 'avdec_h264' },
                    { id: 'avdec_mpeg2video', label: 'MPEG-2 Video - libav (avdec_mpeg2video)', element: 'avdec_mpeg2video' },
                ]);
                updateProgramInputVisibility();
            }
        });
    });
    outputsBody?.addEventListener('input', () => {
        editorDirty = true;
        scheduleRender({ preview: false, meta: true });
    });
    outputsBody?.addEventListener('change', () => {
        editorDirty = true;
        scheduleRender({ preview: false, meta: true });
    });
    addOutputButton?.addEventListener('click', () => {
        const index = outputsBody?.querySelectorAll('[data-output-row]').length || 0;
        outputsBody?.append(outputRow({
            id: `output-${index + 1}`,
            enabled: true,
            destination: 'mpeg_ts_udp',
            video_codec: 'h264',
            audio_codec: fields.audioTopology?.value === 'preserve_native_tracks' ? 'match_input' : 'aac',
            video_bitrate_kbps: '8000',
            audio_bitrate_kbps: '192',
            sample_rate: '48000',
            gop_frames: '60',
            rate_control: 'cbr',
        }));
        updateAudioTopologyVisibility();
        editorDirty = true;
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
    refreshFontsButton?.addEventListener('click', () => {
        refreshFontsButton.disabled = true;
        loadCgenCatalog({ announce: true })
            .catch((error) => setStatus(error.message || 'Unable to refresh CGEN catalog.', 'err'))
            .finally(() => {
                refreshFontsButton.disabled = false;
            });
    });
    fontPicker?.addEventListener('click', toggleFontPicker);
    fontPicker?.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ' || event.key === 'ArrowDown') {
            event.preventDefault();
            setFontPickerOpen(true);
        }
    });
    fontMenu?.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') {
            event.preventDefault();
            setFontPickerOpen(false);
            fontPicker?.focus();
        }
    });
    document.addEventListener('click', (event) => {
        if (!fontMenu || fontMenu.hidden) return;
        const target = event.target;
        if (target instanceof Node && (fontMenu.contains(target) || fontPicker?.contains(target))) {
            return;
        }
        setFontPickerOpen(false);
    });
    document.getElementById('cgenReleaseButton')?.addEventListener('click', () => runAction('release').catch((error) => setStatus(error.message || 'CGEN action failed.', 'err')));
    document.getElementById('cgenOverlayButton')?.addEventListener('click', () => runAction('overlay').catch((error) => setStatus(error.message || 'CGEN action failed.', 'err')));
    document.getElementById('cgenBarsButton')?.addEventListener('click', () => runAction('smpte_bars', { enabled: !fields.smpteBars.checked }).catch((error) => setStatus(error.message || 'CGEN action failed.', 'err')));
    document.getElementById('cgenClockButton')?.addEventListener('click', () => runAction('clock', { enabled: !fields.clockEnabled.checked }).catch((error) => setStatus(error.message || 'CGEN action failed.', 'err')));
    document.getElementById('cgenInsertTextButton')?.addEventListener('click', () => runAction('insert_text', { text: fields.text.value }).catch((error) => setStatus(error.message || 'CGEN action failed.', 'err')));
    document.getElementById('cgenClearTextButton')?.addEventListener('click', () => runAction('clear_text').catch((error) => setStatus(error.message || 'CGEN action failed.', 'err')));
    sunnyButton?.addEventListener('click', () => {
        const action = fields.sunnyCat.checked ? 'banish_sunny' : 'unleash_sunny';
        runAction(action).catch((error) => setStatus(error.message || 'CGEN action failed.', 'err'));
    });
    previewStream?.addEventListener('load', () => {
        previewStream.hidden = false;
        if (preview) preview.hidden = true;
    });
    previewStream?.addEventListener('error', () => {
        previewStream.hidden = true;
        if (preview) preview.hidden = false;
        window.clearTimeout(previewRetryTimer);
        previewRetryTimer = window.setTimeout(() => {
            previewStreamFeedID = '';
            updatePreviewStream(selected());
        }, 2500);
    });
    window.setInterval(() => {
        refreshRuntime().catch(() => scheduleRender({ preview: true, meta: false }));
    }, 1500);
    loadCgenCatalog()
        .catch(() => {
            populateCgenCatalogSelectors();
        })
        .finally(() => {
            loadCgen().catch((error) => setStatus(error.message || 'Unable to load CGEN config.', 'err'));
            loadScenes().catch((error) => {
                renderSceneState('Scene catalog is unavailable.');
                setStatus(error.message || 'Unable to load CGEN scenes.', 'err');
            });
        });
}
