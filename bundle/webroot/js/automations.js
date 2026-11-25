import { panelClient } from './lib/ws-client.js';
import { bindVariableChips } from './lib/variable-chips.js';

const statusBanner = document.getElementById('automationStatusBanner');
const selectEl = document.getElementById('automationSelect');
const countMetric = document.getElementById('automationCountMetric');
const enabledMetric = document.getElementById('automationEnabledMetric');
const summaryList = document.getElementById('automationSummaryList');
const addButton = document.getElementById('automationAddButton');
const saveButton = document.getElementById('automationSaveButton');
const reloadButton = document.getElementById('automationReloadButton');

const fields = {
    name: document.getElementById('automationName'),
    description: document.getElementById('automationDescription'),
    enabled: document.getElementById('automationEnabled'),
    event: document.getElementById('automationEvent'),
    months: document.getElementById('automationMonths'),
    weeks: document.getElementById('automationWeeks'),
    days: document.getElementById('automationDays'),
    weekdays: document.getElementById('automationWeekdays'),
    hours: document.getElementById('automationHours'),
    minutes: document.getElementById('automationMinutes'),
    seconds: document.getElementById('automationSeconds'),
    feeds: document.getElementById('automationFeeds'),
    locations: document.getElementById('automationLocations'),
    duration: document.getElementById('automationDuration'),
    tone: document.getElementById('automationTone'),
};
const langList = document.getElementById('automationLangList');
const langSelect = document.getElementById('automationLangSelect');
const langAddButton = document.getElementById('automationLangAddButton');
const varsBar = document.querySelector('.automation-vars');

let bound = false;
let automations = {};
let selectedKey = '';
// Languages enabled on at least one feed; the backend rejects any other.
let configuredLanguages = [];
let lastTextarea = null;

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[char]));
}

function current() {
    return automations[selectedKey] || null;
}

function weekLines(weeks) {
    return (Array.isArray(weeks) ? weeks : []).map((week) => {
        const value = week.week || week;
        const override = week.event_override || '';
        return override ? `${value}:${override}` : String(value);
    }).join('\n');
}

function parseWeeks(raw) {
    return raw.split(/\r?\n|,/).map((line) => line.trim()).filter(Boolean).map((line) => {
        const [week, override = ''] = line.split(':');
        return { week: week.trim(), event_override: override.trim().toUpperCase() };
    });
}

function splitCSV(raw) {
    return raw.split(',').map((part) => part.trim()).filter(Boolean);
}

function durationParts(raw) {
    const digits = String(raw || '').replace(/\D/g, '').padStart(4, '0').slice(0, 4);
    return { hr: Number(digits.slice(0, 2)) || 0, min: Number(digits.slice(2, 4)) || 0 };
}

// Reads the language blocks as { msg: {code: text}, readers: {code: reader} }.
function readLangBlocks() {
    const msg = {};
    const readers = {};
    langList.querySelectorAll('.automation-lang-block').forEach((block) => {
        const code = block.dataset.lang;
        msg[code] = block.querySelector('textarea').value.trim();
        readers[code] = block.querySelector('input').value.trim();
    });
    return { msg, readers };
}

// The first block is the default; it airs when a feed carries none of the others.
function langBlock(code, text, reader, removable) {
    const block = document.createElement('div');
    block.className = 'automation-lang-block';
    block.dataset.lang = code;
    block.innerHTML = `<div class="automation-lang-head">
            <strong>${escapeHtml(code)}</strong>
            <input type="text" placeholder="reader (auto)" aria-label="Reader ID for ${escapeHtml(code)}">
            ${removable ? '<button class="btn-action" type="button">Remove</button>' : '<small>default language</small>'}
        </div>
        <textarea rows="6"></textarea>`;
    block.querySelector('input').value = reader || '';
    const textarea = block.querySelector('textarea');
    textarea.value = text || '';
    textarea.addEventListener('focus', () => { lastTextarea = textarea; });
    block.querySelector('button')?.addEventListener('click', () => {
        block.remove();
        renderLangSelect();
    });
    return block;
}

function fillLangBlocks(msg, readers) {
    langList.innerHTML = '';
    const codes = Object.keys(msg);
    if (!codes.length) codes.push('en');
    codes.forEach((code, index) => {
        langList.appendChild(langBlock(code, msg[code], readers[code], index > 0));
    });
    lastTextarea = langList.querySelector('textarea');
    renderLangSelect();
}

// Offers only languages some feed has enabled and this automation lacks.
function renderLangSelect() {
    const used = new Set([...langList.querySelectorAll('.automation-lang-block')].map((block) => block.dataset.lang.toLowerCase()));
    const available = configuredLanguages.filter((code) => !used.has(code.toLowerCase()));
    langSelect.innerHTML = available.map((code) => `<option value="${escapeHtml(code)}">${escapeHtml(code)}</option>`).join('');
    langSelect.disabled = available.length === 0;
    langAddButton.disabled = available.length === 0;
}

function readForm() {
    const item = current();
    if (!item) return;
    const previousKey = selectedKey;
    const event = fields.event.value.trim().toUpperCase() || previousKey || 'RWT';
    const duration = durationParts(fields.duration.value);
    const { msg, readers } = readLangBlocks();
    const locations = splitCSV(fields.locations.value).map((id) => ({ id, source: 'eccc' }));
    const feedIds = splitCSV(fields.feeds.value || '*');
    const next = {
        ...item,
        name: fields.name.value.trim() || event,
        description: fields.description.value.trim(),
        automated: {
            enabled: fields.enabled.checked,
            schedule: {
                months: fields.months.value.trim(),
                weeks: parseWeeks(fields.weeks.value),
                days: fields.days.value.trim(),
                weekdays: fields.weekdays.value.trim(),
                hours: fields.hours.value.trim(),
                minutes: fields.minutes.value.trim(),
                seconds: fields.seconds.value.trim(),
            },
            target: { feed_ids: feedIds.length ? feedIds : ['*'] },
        },
        same: {
            ...(item.same || {}),
            enabled: true,
            event,
            locations,
            duration,
            content: {
                ...(item.same?.content || {}),
                attention_tone: fields.tone.value.trim().toUpperCase() || 'WXR',
                lang: msg,
                file: {},
            },
        },
        sameEvent: event,
        sameExpire: `${String(duration.hr).padStart(2, '0')}${String(duration.min).padStart(2, '0')}`,
        msg,
        readers,
        files: {},
    };
    delete automations[previousKey];
    automations[event] = next;
    selectedKey = event;
}

function fillForm() {
    const item = current();
    if (!item) return;
    const automated = item.automated || {};
    const schedule = automated.schedule || {};
    const target = automated.target || {};
    const same = item.same || {};
    const content = same.content || {};
    const duration = same.duration || {};
    fields.name.value = item.name || selectedKey;
    fields.description.value = item.description || '';
    fields.enabled.checked = Boolean(automated.enabled);
    fields.event.value = same.event || item.sameEvent || selectedKey;
    fields.months.value = schedule.months || '';
    fields.weeks.value = weekLines(schedule.weeks);
    fields.days.value = schedule.days || '';
    fields.weekdays.value = schedule.weekdays || '';
    fields.hours.value = schedule.hours || '';
    fields.minutes.value = schedule.minutes || '';
    fields.seconds.value = schedule.seconds || '';
    fields.feeds.value = (target.feed_ids || ['*']).join(',');
    fields.locations.value = (same.locations || []).map((loc) => loc.id || loc).filter(Boolean).join(',');
    fields.duration.value = item.sameExpire || `${String(duration.hr || 0).padStart(2, '0')}${String(duration.min || 15).padStart(2, '0')}`;
    fields.tone.value = content.attention_tone || 'WXR';
    fillLangBlocks(item.msg || content.lang || {}, item.readers || {});
}

function render() {
    const keys = Object.keys(automations).sort();
    if (!selectedKey || !automations[selectedKey]) selectedKey = keys[0] || '';
    selectEl.innerHTML = keys.map((key) => `<option value="${escapeHtml(key)}">${escapeHtml(automations[key]?.name || key)}</option>`).join('');
    selectEl.value = selectedKey;
    countMetric.textContent = String(keys.length);
    enabledMetric.textContent = String(keys.filter((key) => automations[key]?.automated?.enabled).length);
    summaryList.innerHTML = keys.length ? keys.map((key) => {
        const item = automations[key] || {};
        const schedule = item.automated?.schedule || {};
        const weeks = weekLines(schedule.weeks).replace(/\n/g, ', ') || 'any';
        return `<article class="automation-summary-item">
            <strong>${escapeHtml(item.name || key)}</strong>
            <span>${escapeHtml(key)} · ${item.automated?.enabled ? 'enabled' : 'disabled'}</span>
            <small>${escapeHtml(schedule.weekdays || 'any day')} at ${escapeHtml(schedule.hours || '*')}:${escapeHtml(schedule.minutes || '*')}:${escapeHtml(schedule.seconds || '*')} · weeks ${escapeHtml(weeks)}</small>
        </article>`;
    }).join('') : '<article class="playlist-empty">No automations configured.</article>';
    fillForm();
}

async function loadAutomations() {
    statusBanner.textContent = 'Loading automations...';
    statusBanner.dataset.state = 'pending';
    const [loaded, languages] = await Promise.all([
        panelClient.command('automations.get', {}, 8000),
        panelClient.command('automations.languages', {}, 8000),
    ]);
    automations = loaded || {};
    configuredLanguages = languages?.languages || [];
    render();
    statusBanner.textContent = 'Automations loaded.';
    statusBanner.dataset.state = 'ok';
}

async function saveAutomations() {
    readForm();
    statusBanner.textContent = 'Saving automations...';
    statusBanner.dataset.state = 'pending';
    automations = await panelClient.command('automations.put', { content: JSON.stringify(automations) }, 10000) || {};
    render();
    statusBanner.textContent = 'Automations saved. Restart Haze for scheduler changes to take effect.';
    statusBanner.dataset.state = 'ok';
}

function addAutomation() {
    readForm();
    let index = 1;
    let key = 'RWT';
    while (automations[key]) {
        index += 1;
        key = `AUTO${index}`;
    }
    automations[key] = {
        name: 'New Automation',
        description: '',
        automated: {
            enabled: false,
            schedule: { months: '', weeks: [], days: '', weekdays: '', hours: '', minutes: '', seconds: '0' },
            target: { feed_ids: ['*'] },
        },
        same: {
            enabled: true,
            event: key,
            locations: [],
            duration: { hr: 0, min: 15 },
            content: { attention_tone: 'WXR', lang: { en: '' }, file: {} },
        },
        sameEvent: key,
        sameExpire: '0015',
        msg: { en: '' },
        readers: {},
        files: {},
    };
    selectedKey = key;
    render();
}

export function initAutomationsView() {
    if (bound) {
        loadAutomations().catch((error) => {
            statusBanner.textContent = error.message || 'Unable to load automations.';
            statusBanner.dataset.state = 'err';
        });
        return;
    }
    bound = true;
    selectEl.addEventListener('change', () => {
        readForm();
        selectedKey = selectEl.value;
        fillForm();
    });
    addButton.addEventListener('click', addAutomation);
    langAddButton.addEventListener('click', () => {
        if (!langSelect.value) return;
        langList.appendChild(langBlock(langSelect.value, '', '', true));
        renderLangSelect();
    });
    bindVariableChips(varsBar, () => lastTextarea);
    saveButton.addEventListener('click', () => {
        saveAutomations().catch((error) => {
            statusBanner.textContent = error.message || 'Unable to save automations.';
            statusBanner.dataset.state = 'err';
        });
    });
    reloadButton.addEventListener('click', () => {
        loadAutomations().catch((error) => {
            statusBanner.textContent = error.message || 'Unable to load automations.';
            statusBanner.dataset.state = 'err';
        });
    });
    loadAutomations().catch((error) => {
        statusBanner.textContent = error.message || 'Unable to load automations.';
        statusBanner.dataset.state = 'err';
    });
}
