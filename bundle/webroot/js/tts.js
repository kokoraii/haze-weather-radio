import { panelClient } from './lib/ws-client.js';
import { pcmToWav } from './lib/audio.js';

const statusBanner = document.getElementById('ttsStatusBanner');
const tableBody = document.getElementById('ttsReaderTableBody');
const countMetric = document.getElementById('ttsCountMetric');
const providerMetric = document.getElementById('ttsProviderMetric');
const pathLabel = document.getElementById('ttsPathLabel');
const selectedLabel = document.getElementById('ttsSelectedLabel');
const addButton = document.getElementById('ttsAddButton');
const deleteButton = document.getElementById('ttsDeleteButton');
const saveButton = document.getElementById('ttsSaveButton');
const previewButton = document.getElementById('ttsPreviewButton');
const previewAudio = document.getElementById('ttsPreviewAudio');
const previewMeta = document.getElementById('ttsPreviewMeta');

const fields = {
    id: document.getElementById('ttsReaderID'),
    provider: document.getElementById('ttsProvider'),
    voiceID: document.getElementById('ttsVoiceID'),
    language: document.getElementById('ttsLanguage'),
    gender: document.getElementById('ttsGender'),
    backupProvider: document.getElementById('ttsBackupProvider'),
    backupVoiceID: document.getElementById('ttsBackupVoiceID'),
    backupReaderIDs: document.getElementById('ttsBackupReaderIDs'),
    volume: document.getElementById('ttsPreviewVolume'),
    rate: document.getElementById('ttsPreviewRate'),
    previewText: document.getElementById('ttsPreviewText'),
};

let bound = false;
let readers = [];
let providers = ['auto', 'fast', 'piper', 'kokoro', 'sapi5', 'espeak', 'speakyapi', 'f5tts', 'chatterbox'];
let selectedID = '';
let objectUrl = null;

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
    const clean = String(value || '').trim().replace(/[^a-zA-Z0-9_.-]+/g, '-').replace(/^-+|-+$/g, '');
    return clean || `reader-${Date.now().toString(36)}`;
}

function selected() {
    return readers.find((reader) => reader.id === selectedID) || readers[0] || null;
}

function readEditor() {
    return {
        id: sanitizeID(fields.id.value),
        provider: fields.provider.value || 'auto',
        gender: fields.gender.value || 'male',
        language: fields.language.value.trim() || 'en-us',
        voice_id: fields.voiceID.value.trim(),
        backup_provider: fields.backupProvider.value || '',
        backup_voice_id: fields.backupVoiceID.value.trim(),
        backup_reader_ids: fields.backupReaderIDs.value.split(/[,;\r\n]+/).map((value) => value.trim()).filter(Boolean),
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

function writeEditor(reader) {
    const empty = !reader;
    Object.values(fields).forEach((field) => {
        if (field !== fields.previewText && field) field.disabled = empty;
    });
    deleteButton.disabled = empty;
    saveButton.disabled = empty;
    previewButton.disabled = empty;
    if (!reader) {
        fields.id.value = '';
        fields.provider.value = providers[0] || 'auto';
        fields.voiceID.value = '';
        fields.language.value = '';
        fields.gender.value = 'male';
        fields.backupProvider.value = '';
        fields.backupVoiceID.value = '';
        fields.backupReaderIDs.value = '';
        selectedLabel.textContent = 'No reader selected';
        providerMetric.textContent = 'none';
        revokePreview();
        return;
    }
    fields.id.value = reader.id || '';
    fields.provider.value = reader.provider || 'auto';
    fields.voiceID.value = reader.voice_id || '';
    fields.language.value = reader.language || 'en-us';
    fields.gender.value = reader.gender || 'male';
    fields.backupProvider.value = reader.backup_provider || '';
    fields.backupVoiceID.value = reader.backup_voice_id || '';
    fields.backupReaderIDs.value = Array.isArray(reader.backup_reader_ids) ? reader.backup_reader_ids.join(', ') : '';
    selectedLabel.textContent = reader.id || 'New reader';
    providerMetric.textContent = reader.provider || 'auto';
}

function renderProviders() {
    const current = fields.provider.value;
    const currentBackup = fields.backupProvider.value;
    fields.provider.innerHTML = providers.map((provider) => (
        `<option value="${escapeHtml(provider)}">${escapeHtml(provider)}</option>`
    )).join('');
    fields.backupProvider.innerHTML = '<option value="">No backup</option>' + providers
        .filter((provider) => provider !== 'auto')
        .map((provider) => `<option value="${escapeHtml(provider)}">${escapeHtml(provider)}</option>`)
        .join('');
    if (providers.includes(current)) fields.provider.value = current;
    if (providers.includes(currentBackup)) fields.backupProvider.value = currentBackup;
}

function renderTable() {
    countMetric.textContent = String(readers.length);
    if (!readers.length) {
        tableBody.innerHTML = '<tr><td colspan="3" class="panel-empty-cell">No readers configured.</td></tr>';
        writeEditor(null);
        return;
    }
    if (!selectedID || !readers.some((reader) => reader.id === selectedID)) {
        selectedID = readers[0].id;
    }
    tableBody.innerHTML = readers.map((reader) => {
        const active = reader.id === selectedID;
        return `
        <tr class="tts-reader-row ${active ? 'active' : ''}" data-reader-id="${escapeHtml(reader.id)}" aria-selected="${active ? 'true' : 'false'}" tabindex="0">
            <td>
                <div class="tts-reader-main">
                    <strong>${escapeHtml(reader.id)}</strong>
                    <span>${escapeHtml(reader.gender || 'male')} · ${escapeHtml(reader.language || 'en-us')}</span>
                </div>
            </td>
            <td><span class="table-pill" data-state="tts">${escapeHtml(reader.provider || 'auto')}</span>${reader.backup_provider ? `<span class="table-muted"> → ${escapeHtml(reader.backup_provider)}</span>` : ''}${Array.isArray(reader.backup_reader_ids) && reader.backup_reader_ids.length ? `<span class="table-muted"> for ${escapeHtml(reader.backup_reader_ids.join(', '))}</span>` : ''}</td>
            <td title="${escapeHtml(reader.voice_id || 'Auto voice')}${reader.backup_voice_id ? `, inline backup ${escapeHtml(reader.backup_voice_id)}` : ''}">${reader.voice_id ? escapeHtml(reader.voice_id) : '<span class="table-muted">Auto voice</span>'}</td>
        </tr>
    `;
    }).join('');
    writeEditor(selected());
}

function setPreviewBusy(busy, message = '') {
    previewButton.disabled = busy || !selected();
    if (message) previewMeta.textContent = message;
}

function revokePreview() {
    if (objectUrl) URL.revokeObjectURL(objectUrl);
    objectUrl = null;
    previewAudio.pause();
    previewAudio.removeAttribute('src');
    previewAudio.hidden = true;
}

function audioBytes(result) {
    const raw = atob(result.audio_base64 || '');
    const bytes = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i += 1) bytes[i] = raw.charCodeAt(i);
    if (result.format === 'raw') {
        return pcmToWav(bytes, result.sample_rate || 48000, result.channels || 1);
    }
    return bytes;
}

async function loadTTS() {
    setStatus('Loading TTS readers...', 'pending');
    const payload = await panelClient.command('tts.get', {}, 10000);
    readers = Array.isArray(payload.readers) ? payload.readers : [];
    providers = Array.isArray(payload.providers) && payload.providers.length ? payload.providers : providers;
    pathLabel.textContent = payload.configured || payload.path || 'managed/configs/readers.xml';
    if (!fields.previewText.value) fields.previewText.value = payload.preview_text || '';
    renderProviders();
    renderTable();
    setStatus(`Loaded ${readers.length} reader${readers.length === 1 ? '' : 's'}.`, 'ok');
}

async function saveTTS() {
    updateSelectedFromEditor();
    setStatus('Saving TTS readers...', 'pending');
    const payload = await panelClient.command('tts.save', { readers }, 15000);
    readers = Array.isArray(payload.readers) ? payload.readers : readers;
    pathLabel.textContent = payload.configured || payload.path || pathLabel.textContent;
    renderTable();
    setStatus('Saved readers.xml.', 'ok');
}

async function previewTTS() {
    updateSelectedFromEditor();
    const current = selected();
    if (!current) return;
    revokePreview();
    setPreviewBusy(true, 'Generating preview...');
    try {
        const result = await panelClient.command('tts.preview', {
            reader_id: '',
            provider: current.provider,
            voice_id: current.voice_id,
            language: current.language,
            text: fields.previewText.value,
            volume: Number(fields.volume.value || 100),
            rate: Number(fields.rate.value || 0),
        }, 120000);
        const bytes = audioBytes(result);
        objectUrl = URL.createObjectURL(new Blob([bytes], { type: result.content_type || 'audio/wav' }));
        previewAudio.src = objectUrl;
        previewAudio.hidden = false;
        previewMeta.textContent = `Preview generated with ${result.provider || current.provider || 'auto'}${result.voice_id ? ` · ${result.voice_id}` : ''}.`;
        try {
            await previewAudio.play();
        } catch {
            previewMeta.textContent += ' Press play to listen.';
        }
    } finally {
        setPreviewBusy(false);
    }
}

function addReader() {
    updateSelectedFromEditor();
    let index = readers.length;
    let id = String(index).padStart(2, '0');
    while (readers.some((reader) => reader.id === id)) {
        index += 1;
        id = String(index).padStart(2, '0');
    }
    readers.push({
        id,
        provider: 'auto',
        gender: 'male',
        language: 'en-us',
        voice_id: '',
        backup_provider: '',
        backup_voice_id: '',
        backup_reader_ids: [],
    });
    selectedID = id;
    renderTable();
    fields.voiceID.focus();
}

function deleteReader() {
    const current = selected();
    if (!current) return;
    readers = readers.filter((reader) => reader.id !== current.id);
    selectedID = readers[0]?.id || '';
    renderTable();
    setStatus(`Removed ${current.id}. Save XML to persist.`, 'pending');
}

function bind() {
    if (bound) return;
    bound = true;
    tableBody.addEventListener('click', (event) => {
        const row = event.target.closest('[data-reader-id]');
        if (!row) return;
        updateSelectedFromEditor();
        selectedID = row.dataset.readerId;
        renderTable();
    });
    tableBody.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter' && event.key !== ' ') return;
        const row = event.target.closest('[data-reader-id]');
        if (!row) return;
        event.preventDefault();
        updateSelectedFromEditor();
        selectedID = row.dataset.readerId;
        renderTable();
    });
    Object.values(fields).forEach((field) => {
        if (!field || field === fields.previewText) return;
        field.addEventListener('input', () => {
            updateSelectedFromEditor();
            selectedLabel.textContent = selected()?.id || 'No reader selected';
            providerMetric.textContent = selected()?.provider || 'auto';
            setStatus('Unsaved TTS reader changes.', 'pending');
        });
        field.addEventListener('change', () => {
            updateSelectedFromEditor();
            renderTable();
            setStatus('Unsaved TTS reader changes.', 'pending');
        });
    });
    addButton.addEventListener('click', addReader);
    deleteButton.addEventListener('click', deleteReader);
    saveButton.addEventListener('click', () => saveTTS().catch((err) => setStatus(err.message, 'err')));
    previewButton.addEventListener('click', () => previewTTS().catch((err) => {
        setPreviewBusy(false, err.message);
        setStatus(err.message, 'err');
    }));
}

export function initTTSView() {
    bind();
    loadTTS().catch((err) => setStatus(err.message, 'err'));
}
