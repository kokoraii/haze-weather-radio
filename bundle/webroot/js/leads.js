import { panelClient } from './lib/ws-client.js';

const byID = (id) => document.getElementById(id);

const statusBanner = byID('leadStatementsStatus');
const countMetric = byID('leadStatementsCount');
const enabledMetric = byID('leadStatementsEnabled');
const pathLabel = byID('leadStatementsPath');
const statementList = byID('leadStatementList');
const addButton = byID('leadStatementAddButton');
const saveButton = byID('leadStatementsSaveButton');
const deleteButton = byID('leadStatementDeleteButton');
const addConditionButton = byID('leadConditionAddButton');
const conditionsBody = byID('leadConditionsBody');
const audioFiles = byID('leadStatementAudioFiles');
const previewButton = byID('leadStatementPreviewButton');
const previewSameButton = byID('leadStatementPreviewSameButton');
const previewAudio = byID('leadStatementPreviewAudio');
const previewMeta = byID('leadStatementPreviewMeta');
const editorTitle = byID('leadStatementEditorTitle');

const fields = {
    name: byID('leadStatementName'),
    enabled: byID('leadStatementEnabled'),
    leadIn: byID('leadStatementLeadIn'),
    leadOut: byID('leadStatementLeadOut'),
};

let bound = false;
let statements = [];
let selectedIndex = -1;
let previewObjectURL = null;

function setStatus(text, state = 'ok') {
    if (!statusBanner) return;
    statusBanner.textContent = text;
    statusBanner.dataset.state = state;
}

function clone(value) {
    return JSON.parse(JSON.stringify(value));
}

function currentStatement() {
    return selectedIndex >= 0 && selectedIndex < statements.length ? statements[selectedIndex] : null;
}

function displayName(statement, fallback = 'Unnamed lead') {
    const name = String(statement?.name || '').trim();
    return name || fallback;
}

function defaultStatement() {
    let number = statements.length + 1;
    let name = 'New lead statement';
    const existing = new Set(statements.map((statement) => String(statement.name || '').trim().toLowerCase()));
    while (existing.has(name.toLowerCase())) {
        number += 1;
        name = `New lead statement ${number}`;
    }
    return {
        enabled: true,
        name,
        lead_in: '',
        lead_out: '',
        conditions: [],
    };
}

function setEditorDisabled(disabled) {
    Object.values(fields).forEach((field) => {
        if (field) field.disabled = disabled;
    });
    if (deleteButton) deleteButton.disabled = disabled;
    if (addConditionButton) addConditionButton.disabled = disabled;
    if (previewButton) previewButton.disabled = disabled;
    if (previewSameButton) previewSameButton.disabled = disabled;
}

function clearPreview() {
    if (previewObjectURL) URL.revokeObjectURL(previewObjectURL);
    previewObjectURL = null;
    if (!previewAudio) return;
    previewAudio.pause();
    previewAudio.removeAttribute('src');
    previewAudio.hidden = true;
}

function option(value, label, selected = false) {
    const item = document.createElement('option');
    item.value = value;
    item.textContent = label;
    item.selected = selected;
    return item;
}

function selectWithOptions(options, value, className = '') {
    const select = document.createElement('select');
    if (className) select.className = className;
    options.forEach(([optionValue, optionLabel]) => select.append(option(optionValue, optionLabel, optionValue === value)));
    if (!options.some(([optionValue]) => optionValue === value)) select.value = options[0][0];
    return select;
}

function conditionTarget(condition) {
    return condition?.location ? 'location' : 'key';
}

function conditionValue(condition) {
    return String(condition?.key || condition?.location || '');
}

function conditionOperator(condition) {
    for (const name of ['equals', 'includes', 'startswith', 'endswith']) {
        if (String(condition?.[name] || '').trim()) return name;
    }
    return 'equals';
}

function conditionExpected(condition) {
    return String(condition?.[conditionOperator(condition)] || '');
}

function cell(child, className = '') {
    const item = document.createElement('td');
    if (className) item.className = className;
    if (child) item.append(child);
    return item;
}

function updateConditionRow(row) {
    const type = row.querySelector('[data-lead-condition-type]')?.value || 'if';
    const isMatch = type === 'if';
    row.querySelectorAll('[data-lead-condition-field]').forEach((element) => {
        element.hidden = !isMatch;
        element.querySelectorAll('input, select').forEach((control) => {
            control.disabled = !isMatch;
        });
    });
    const join = row.querySelector('[data-lead-condition-join]');
    if (join) join.hidden = isMatch;
}

function renderConditions(conditions) {
    if (!conditionsBody) return;
    conditionsBody.replaceChildren();
    if (!conditions.length) {
        const row = document.createElement('tr');
        const item = document.createElement('td');
        item.colSpan = 7;
        item.className = 'panel-empty-cell';
        item.textContent = 'No conditions, this lead matches every alert when enabled.';
        row.append(item);
        conditionsBody.append(row);
        return;
    }
    conditions.forEach((condition) => {
        const row = document.createElement('tr');
        const type = String(condition?.type || 'if').toLowerCase();
        const typeSelect = selectWithOptions([
            ['if', 'Match'],
            ['and', 'AND'],
            ['or', 'OR'],
        ], ['if', 'and', 'or'].includes(type) ? type : 'if', 'lead-condition-rule');
        typeSelect.dataset.leadConditionType = 'true';
        row.append(cell(typeSelect));

        const join = document.createElement('td');
        join.colSpan = 5;
        join.className = 'lead-condition-join';
        join.dataset.leadConditionJoin = 'true';
        join.textContent = 'Joins the surrounding match rules.';
        row.append(join);

        const targetSelect = selectWithOptions([
            ['key', 'CAP field'],
            ['location', 'Location'],
        ], conditionTarget(condition), 'lead-condition-target');
        targetSelect.dataset.leadConditionTarget = 'true';
        row.append(cell(targetSelect, 'lead-condition-cell'));
        row.lastElementChild.dataset.leadConditionField = 'true';

        const targetInput = document.createElement('input');
        targetInput.type = 'text';
        targetInput.maxLength = 256;
        targetInput.placeholder = 'layer:SOREM:1.0:Broadcast_Immediately';
        targetInput.value = conditionValue(condition);
        targetInput.dataset.leadConditionTargetValue = 'true';
        row.append(cell(targetInput, 'lead-condition-cell'));
        row.lastElementChild.dataset.leadConditionField = 'true';

        const operatorSelect = selectWithOptions([
            ['equals', 'equals'],
            ['includes', 'includes'],
            ['startswith', 'starts with'],
            ['endswith', 'ends with'],
        ], conditionOperator(condition));
        operatorSelect.dataset.leadConditionOperator = 'true';
        row.append(cell(operatorSelect, 'lead-condition-cell'));
        row.lastElementChild.dataset.leadConditionField = 'true';

        const expectedInput = document.createElement('input');
        expectedInput.type = 'text';
        expectedInput.maxLength = 256;
        expectedInput.placeholder = 'Yes';
        expectedInput.value = conditionExpected(condition);
        expectedInput.dataset.leadConditionExpected = 'true';
        row.append(cell(expectedInput, 'lead-condition-cell lead-condition-value'));
        row.lastElementChild.dataset.leadConditionField = 'true';

        const optionsBox = document.createElement('div');
        optionsBox.className = 'lead-condition-checkboxes';
        [
            ['matchcase', 'Case', Boolean(condition?.matchcase)],
            ['matchwhole', 'Whole', Boolean(condition?.matchwhole)],
            ['useregex', 'Regex', Boolean(condition?.useregex)],
        ].forEach(([key, label, checked]) => {
            const optionLabel = document.createElement('label');
            const input = document.createElement('input');
            input.type = 'checkbox';
            input.checked = checked;
            input.dataset.leadConditionOption = key;
            optionLabel.append(input, document.createTextNode(label));
            optionsBox.append(optionLabel);
        });
        row.append(cell(optionsBox, 'lead-condition-cell'));
        row.lastElementChild.dataset.leadConditionField = 'true';

        const remove = document.createElement('button');
        remove.type = 'button';
        remove.className = 'btn-danger';
        remove.textContent = 'Remove';
        remove.dataset.leadConditionRemove = 'true';
        row.append(cell(remove));
        updateConditionRow(row);
        conditionsBody.append(row);
    });
}

function readConditions() {
    if (!conditionsBody) return [];
    const result = [];
    conditionsBody.querySelectorAll('tr').forEach((row) => {
        const type = row.querySelector('[data-lead-condition-type]')?.value;
        if (!type) return;
        if (type === 'and' || type === 'or') {
            result.push({ type });
            return;
        }
        const target = row.querySelector('[data-lead-condition-target]')?.value || 'key';
        const targetValue = String(row.querySelector('[data-lead-condition-target-value]')?.value || '').trim();
        const operator = row.querySelector('[data-lead-condition-operator]')?.value || 'equals';
        const expected = String(row.querySelector('[data-lead-condition-expected]')?.value || '').trim();
        const condition = { type: 'if', [target]: targetValue, [operator]: expected };
        row.querySelectorAll('[data-lead-condition-option]').forEach((input) => {
            if (input.checked) condition[input.dataset.leadConditionOption] = true;
        });
        result.push(condition);
    });
    return result;
}

function commitEditor() {
    if (!currentStatement()) return;
    statements[selectedIndex] = {
        enabled: Boolean(fields.enabled?.checked),
        name: String(fields.name?.value || '').trim(),
        lead_in: String(fields.leadIn?.value || '').trim(),
        lead_out: String(fields.leadOut?.value || '').trim(),
        conditions: readConditions(),
    };
}

function renderEditor() {
    const statement = currentStatement();
    setEditorDisabled(!statement);
    clearPreview();
    if (!statement) {
        editorTitle.textContent = 'No lead statement selected';
        fields.name.value = '';
        fields.enabled.checked = false;
        fields.leadIn.value = '';
        fields.leadOut.value = '';
        renderConditions([]);
        previewMeta.textContent = 'Add a lead statement to create a local preview.';
        return;
    }
    editorTitle.textContent = displayName(statement);
    fields.name.value = statement.name || '';
    fields.enabled.checked = Boolean(statement.enabled);
    fields.leadIn.value = statement.lead_in || '';
    fields.leadOut.value = statement.lead_out || '';
    renderConditions(Array.isArray(statement.conditions) ? statement.conditions : []);
    previewMeta.textContent = 'Previews are generated locally and are never sent to a live feed.';
}

function statementSummary(statement) {
    if (!statement.enabled) return 'Disabled';
    const audio = [statement.lead_in, statement.lead_out].filter(Boolean);
    if (!audio.length) return 'No audio selected';
    const conditions = Array.isArray(statement.conditions) ? statement.conditions.filter((condition) => condition.type === 'if').length : 0;
    return `${conditions ? `${conditions} match rule${conditions === 1 ? '' : 's'} · ` : 'All alerts · '}${audio.length === 2 ? 'Lead in + out' : statement.lead_in ? 'Lead in' : 'Lead out'}`;
}

function renderStatementList() {
    if (!statementList) return;
    countMetric.textContent = String(statements.length);
    enabledMetric.textContent = String(statements.filter((statement) => statement.enabled).length);
    statementList.replaceChildren();
    if (!statements.length) {
        const empty = document.createElement('p');
        empty.className = 'form-hint';
        empty.textContent = 'No lead statements are configured.';
        statementList.append(empty);
        return;
    }
    statements.forEach((statement, index) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = `lead-list-entry${index === selectedIndex ? ' active' : ''}`;
        button.dataset.leadStatementIndex = String(index);
        button.setAttribute('role', 'option');
        button.setAttribute('aria-selected', index === selectedIndex ? 'true' : 'false');
        const title = document.createElement('strong');
        title.textContent = displayName(statement);
        const description = document.createElement('span');
        description.textContent = statementSummary(statement);
        button.append(title, description);
        statementList.append(button);
    });
}

function renderAudioFiles(files) {
    if (!audioFiles) return;
    audioFiles.replaceChildren();
    (Array.isArray(files) ? files : []).forEach((file) => {
        const item = document.createElement('option');
        item.value = String(file);
        audioFiles.append(item);
    });
}

function markDirty() {
    setStatus('Unsaved lead statement changes.', 'pending');
}

function selectStatement(index) {
    commitEditor();
    selectedIndex = index;
    renderStatementList();
    renderEditor();
}

function addStatement() {
    commitEditor();
    statements.push(defaultStatement());
    selectedIndex = statements.length - 1;
    renderStatementList();
    renderEditor();
    fields.name.focus();
    markDirty();
}

function deleteStatement() {
    const current = currentStatement();
    if (!current) return;
    if (!window.confirm(`Delete lead statement "${displayName(current)}"?`)) return;
    statements.splice(selectedIndex, 1);
    selectedIndex = Math.min(selectedIndex, statements.length - 1);
    renderStatementList();
    renderEditor();
    markDirty();
}

function addCondition() {
    const current = currentStatement();
    if (!current) return;
    commitEditor();
    current.conditions = Array.isArray(current.conditions) ? current.conditions : [];
    if (current.conditions.length && current.conditions[current.conditions.length - 1]?.type === 'if') current.conditions.push({ type: 'and' });
    current.conditions.push({ type: 'if', key: '', equals: '' });
    renderEditor();
    markDirty();
}

function removeCondition(button) {
    const row = button.closest('tr');
    if (!row) return;
    row.remove();
    commitEditor();
    renderConditions(currentStatement()?.conditions || []);
    markDirty();
}

function base64Bytes(value) {
    const decoded = window.atob(String(value || ''));
    const bytes = new Uint8Array(decoded.length);
    for (let index = 0; index < decoded.length; index += 1) bytes[index] = decoded.charCodeAt(index);
    return bytes;
}

function setPreviewBusy(busy) {
    const disabled = busy || !currentStatement();
    previewButton.disabled = disabled;
    previewSameButton.disabled = disabled;
}

async function preview(includeSame) {
    commitEditor();
    const statement = currentStatement();
    if (!statement) return;
    clearPreview();
    setPreviewBusy(true);
    setStatus('Generating a local lead-statement preview...', 'pending');
    try {
        const result = await panelClient.command('lead_statements.preview', {
            statement: clone(statement),
            include_same: includeSame,
        }, 120000);
        previewObjectURL = URL.createObjectURL(new Blob([base64Bytes(result.audio_base64)], {
            type: result.content_type || 'audio/wav',
        }));
        previewAudio.src = previewObjectURL;
        previewAudio.hidden = false;
        const sameText = result.include_same ? ` Includes ${result.same_header || 'SAME test audio'}.` : ' No SAME test audio.';
        previewMeta.textContent = `Preview for ${result.statement || displayName(statement)}.${sameText}`;
        setStatus('Lead-statement preview is ready.', 'ok');
        try {
            await previewAudio.play();
        } catch {
            previewMeta.textContent += ' Press play to listen.';
        }
    } catch (error) {
        setStatus(error.message || 'Lead-statement preview failed.', 'err');
        previewMeta.textContent = error.message || 'Lead-statement preview failed.';
    } finally {
        setPreviewBusy(false);
    }
}

async function saveStatements() {
    commitEditor();
    saveButton.disabled = true;
    setStatus('Saving lead statements...', 'pending');
    try {
        const result = await panelClient.command('lead_statements.save', { statements: clone(statements) }, 15000);
        statements = Array.isArray(result.statements) ? result.statements : statements;
        selectedIndex = statements.length ? Math.min(Math.max(selectedIndex, 0), statements.length - 1) : -1;
        pathLabel.textContent = result.path || 'managed/configs/lead.xml';
        renderStatementList();
        renderEditor();
        setStatus('Saved lead.xml. New alerts will use the updated statements.', 'ok');
    } catch (error) {
        setStatus(error.message || 'Lead-statement save failed.', 'err');
    } finally {
        saveButton.disabled = false;
    }
}

async function loadStatements() {
    setStatus('Loading lead statements...', 'pending');
    const result = await panelClient.command('lead_statements.get', {}, 10000);
    statements = Array.isArray(result.statements) ? result.statements : [];
    selectedIndex = statements.length ? 0 : -1;
    pathLabel.textContent = result.path || 'managed/configs/lead.xml';
    renderAudioFiles(result.audio_files);
    renderStatementList();
    renderEditor();
    setStatus(`Loaded ${statements.length} lead statement${statements.length === 1 ? '' : 's'}.`, 'ok');
}

function bind() {
    if (bound) return;
    bound = true;
    statementList.addEventListener('click', (event) => {
        const item = event.target.closest('[data-lead-statement-index]');
        if (!item) return;
        selectStatement(Number(item.dataset.leadStatementIndex));
    });
    Object.values(fields).forEach((field) => {
        field.addEventListener('input', () => {
            commitEditor();
            editorTitle.textContent = displayName(currentStatement());
            markDirty();
        });
        field.addEventListener('change', () => {
            commitEditor();
            renderStatementList();
            editorTitle.textContent = displayName(currentStatement());
            markDirty();
        });
    });
    conditionsBody.addEventListener('change', (event) => {
        const row = event.target.closest('tr');
        if (!row) return;
        if (event.target.matches('[data-lead-condition-type]')) updateConditionRow(row);
        commitEditor();
        markDirty();
    });
    conditionsBody.addEventListener('input', () => {
        commitEditor();
        markDirty();
    });
    conditionsBody.addEventListener('click', (event) => {
        const button = event.target.closest('[data-lead-condition-remove]');
        if (button) removeCondition(button);
    });
    addButton.addEventListener('click', addStatement);
    deleteButton.addEventListener('click', deleteStatement);
    addConditionButton.addEventListener('click', addCondition);
    saveButton.addEventListener('click', saveStatements);
    previewButton.addEventListener('click', () => preview(false));
    previewSameButton.addEventListener('click', () => preview(true));
}

export function initLeadStatementsView() {
    bind();
    loadStatements().catch((error) => setStatus(error.message || 'Lead statements could not be loaded.', 'err'));
}
