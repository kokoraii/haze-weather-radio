import { apiCommand, session } from './lib/api.js';

const byID = (id) => document.getElementById(id);

const form = byID('locationUtilityForm');
const operation = byID('locationOperation');
const statusBanner = byID('locationUtilityStatus');
const summary = byID('locationResultSummary');
const resultList = byID('locationResultList');
const rawResponse = byID('locationRawResponse');
const rawJSON = byID('locationRawJSON');
const queryButton = byID('locationQueryButton');
const clearButton = byID('locationClearButton');
const inputLabel = byID('locationInputLabel');
const inputHelp = byID('locationInputHelp');
const catalogImportForm = byID('locationCatalogImportForm');
const catalogImportFile = byID('locationCatalogImportFile');
const catalogImportButton = byID('locationCatalogImportButton');
const catalogImportStatus = byID('locationCatalogImportStatus');

let bound = false;

const operationHelp = {
    search: ['Location name', 'Search canonical and alternate names using deterministic multilingual ranking.'],
    resolve: ['Identifier value', 'Choose a namespace for an exact lookup, or leave the scheme blank for ambiguity-safe automatic recognition.'],
    batch_resolve: ['', 'Enter up to 100 identifiers. Use scheme:value for qualified codes, or a bare value for automatic recognition.'],
    nearest: ['', 'Find the closest compatible entities to a WGS84 coordinate using spatial filtering and geodesic ranking.'],
    point_facets: ['', 'Return containing and nearby location roles grouped into point facets.'],
    traverse: ['Canonical entity ID', 'Traverse only the explicit relationship types supplied below.'],
};

function setStatus(text, state = 'pending') {
    if (!statusBanner) return;
    statusBanner.textContent = text;
    statusBanner.dataset.state = state;
}

function setFieldVisible(name, visible) {
    document.querySelectorAll(`[data-location-field="${name}"]`).forEach((field) => {
        field.hidden = !visible;
    });
}

function updateOperationFields() {
    const selected = operation?.value || 'search';
    const textInput = selected === 'search' || selected === 'resolve' || selected === 'traverse';
    setFieldVisible('text', textInput);
    setFieldVisible('scheme', selected === 'resolve');
    setFieldVisible('authority', selected === 'resolve');
    setFieldVisible('batch', selected === 'batch_resolve');
    setFieldVisible('latitude', selected === 'nearest' || selected === 'point_facets');
    setFieldVisible('longitude', selected === 'nearest' || selected === 'point_facets');
    setFieldVisible('relationships', selected === 'traverse');
    setFieldVisible('depth', selected === 'traverse');
    setFieldVisible('visited', selected === 'traverse');
    setFieldVisible('input-mode', selected === 'search');
    const [label, help] = operationHelp[selected] || operationHelp.search;
    if (inputLabel) inputLabel.textContent = label;
    if (inputHelp) inputHelp.textContent = help;
    const input = byID('locationInputText');
    if (!input) return;
    input.placeholder = selected === 'resolve'
        ? 'CYXE'
        : selected === 'traverse'
            ? 'urn:haze:location:...'
            : 'Saskatoon';
}

function commaList(id) {
    return String(byID(id)?.value || '')
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);
}

function optionalNumber(id) {
    const raw = String(byID(id)?.value || '').trim();
    if (!raw) return null;
    const value = Number(raw);
    if (!Number.isFinite(value)) throw new Error(`${byID(id)?.previousElementSibling?.textContent || 'Number'} is invalid.`);
    return value;
}

function qualifiedOrAutoInput(raw) {
    const text = String(raw || '').trim();
    const separator = text.indexOf(':');
    if (separator > 0 && !text.toLowerCase().startsWith('urn:haze:')) {
        const scheme = text.slice(0, separator).trim().toLowerCase();
        const value = text.slice(separator + 1).trim();
        if (/^[a-z][a-z0-9_-]*$/.test(scheme) && value) {
            return { kind: 'identifier', scheme, value };
        }
    }
    return { kind: 'auto', text };
}

function buildInput(selected) {
    if (selected === 'batch_resolve') return null;
    if (selected === 'nearest' || selected === 'point_facets') {
        const latitude = optionalNumber('locationLatitude');
        const longitude = optionalNumber('locationLongitude');
        if (latitude === null || longitude === null) throw new Error('Latitude and longitude are required.');
        return { kind: 'point', latitude, longitude };
    }
    const text = String(byID('locationInputText')?.value || '').trim();
    if (!text) throw new Error(selected === 'traverse' ? 'Canonical entity ID is required.' : 'Location input is required.');
    if (selected === 'search') return { kind: 'name', text };
    if (selected === 'traverse') return { kind: 'entity', id: text };
    const scheme = String(byID('locationScheme')?.value || '').trim().toLowerCase();
    const authority = String(byID('locationAuthority')?.value || '').trim().toLowerCase();
    if (!scheme) return { kind: 'auto', text };
    return { kind: 'identifier', scheme, value: text, ...(authority ? { authority } : {}) };
}

function buildBatchInputs() {
    const inputs = String(byID('locationBatchInputs')?.value || '')
        .split(/\r?\n/)
        .map((value) => value.trim())
        .filter(Boolean)
        .map(qualifiedOrAutoInput);
    if (!inputs.length) throw new Error('At least one batch identifier is required.');
    if (inputs.length > 100) throw new Error('Batch queries are limited to 100 identifiers.');
    return inputs;
}

function buildRequest() {
    const selected = operation?.value || 'search';
    const filters = {
        kinds: commaList('locationKinds'),
        capabilities: commaList('locationCapabilities'),
        country: String(byID('locationCountry')?.value || '').trim(),
        region: String(byID('locationRegion')?.value || '').trim(),
        relationship_types: commaList('locationRelationships'),
    };
    const options = {
        limit: optionalNumber('locationLimit') || 10,
        minimum_confidence: byID('locationMinimumConfidence')?.value || 'medium',
        include_inactive: Boolean(byID('locationIncludeInactive')?.checked),
        dedupe_mode: byID('locationDedupe')?.value || 'none',
        expand_members: Boolean(byID('locationExpandMembers')?.checked),
        station_mode_requirement: byID('locationStationRequirement')?.value || 'any',
        locale: String(byID('locationLocale')?.value || '').trim(),
        input_mode: byID('locationInputMode')?.value || 'text',
    };
    const maxDistance = optionalNumber('locationMaxDistance');
    if (maxDistance !== null) options.max_distance_km = maxDistance;
    const preference = byID('locationStationPreference')?.value || '';
    if (preference) options.station_mode_preference = preference;
    if (selected === 'traverse') {
        options.max_depth = optionalNumber('locationMaxDepth') || 1;
        options.max_visited = optionalNumber('locationMaxVisited') || 10000;
    }
    const request = { api_version: 1, operation: selected, filters, options };
    if (selected === 'batch_resolve') request.inputs = buildBatchInputs();
    else request.input = buildInput(selected);
    return request;
}

function textElement(tag, text, className = '') {
    const element = document.createElement(tag);
    if (className) element.className = className;
    element.textContent = String(text ?? '');
    return element;
}

function displayName(entity = {}) {
    const names = Array.isArray(entity.names) ? entity.names : [];
    return names.find((name) => name?.primary)?.value || names[0]?.value || entity.id || 'Unnamed location';
}

function identifierText(entity = {}) {
    const identifiers = Array.isArray(entity.identifiers) ? entity.identifiers : [];
    return identifiers.map((item) => `${item.scheme || 'id'}:${item.value || ''}`).join('  |  ');
}

function renderCandidate(candidate, contextLabel = '') {
    const entity = candidate?.entity || {};
    const card = document.createElement('article');
    card.className = 'location-result-card';
    const heading = document.createElement('div');
    heading.className = 'location-result-heading';
    const title = document.createElement('div');
    title.append(textElement('h3', displayName(entity)));
    title.append(textElement('code', entity.id || 'No canonical ID'));
    heading.append(title);
    const score = Number(candidate?.match?.score);
    const scoreText = Number.isFinite(score) ? `${candidate.match.confidence || 'unknown'} ${(score * 100).toFixed(1)}%` : candidate?.match?.confidence || 'unknown';
    heading.append(textElement('span', scoreText, 'location-result-score'));
    card.append(heading);

    const meta = document.createElement('div');
    meta.className = 'location-result-meta';
    [
        contextLabel,
        entity.kind,
        [entity.country, entity.region].filter(Boolean).join('/'),
        candidate?.facet,
        Number.isFinite(Number(candidate?.distance_m)) ? `${Number(candidate.distance_m).toFixed(0)} m` : '',
        candidate?.match?.method,
    ].filter(Boolean).forEach((value) => meta.append(textElement('span', value)));
    if (candidate?.grouping?.member_count > 1) {
        meta.append(textElement('span', `${candidate.grouping.member_count} grouped facilities`));
    }
    card.append(meta);
    const identifiers = identifierText(entity);
    if (identifiers) card.append(textElement('div', identifiers, 'location-result-identifiers'));
    if (candidate?.grouping?.member_ids?.length) {
        card.append(textElement('div', `Members: ${candidate.grouping.member_ids.join(', ')}`, 'location-result-identifiers'));
    }
    if (candidate?.path?.length) {
        const path = candidate.path.map((step) => `${step.from_id} --${step.relationship_type}--> ${step.to_id}`).join('\n');
        card.append(textElement('div', path, 'location-result-identifiers'));
    }
    return card;
}

function appendSummary(label, value) {
    const box = document.createElement('div');
    box.append(textElement('dt', label));
    box.append(textElement('dd', value));
    summary.append(box);
}

function renderResponse(response) {
    summary.replaceChildren();
    const batches = Array.isArray(response?.batches) ? response.batches : [];
    const results = Array.isArray(response?.results) ? response.results : [];
    const resultCount = batches.length
        ? batches.reduce((count, batch) => count + (Array.isArray(batch.results) ? batch.results.length : 0), 0)
        : results.length;
    appendSummary('Status', response?.status || 'unknown');
    appendSummary('Results', resultCount);
    appendSummary('Catalog', response?.catalog_generation || 'unknown');
    appendSummary('Ambiguity', response?.ambiguous ? 'ambiguous' : 'clear');
    summary.hidden = false;
    resultList.replaceChildren();
    if (batches.length) {
        batches.forEach((batch) => {
            const candidates = Array.isArray(batch.results) ? batch.results : [];
            candidates.forEach((candidate) => resultList.append(renderCandidate(candidate, `Input ${Number(batch.input_index) + 1}`)));
        });
    } else {
        results.forEach((candidate) => resultList.append(renderCandidate(candidate)));
    }
    if (!resultList.children.length) {
        resultList.append(textElement('div', 'No locations matched this query.', 'location-result-empty'));
    }
    rawJSON.textContent = JSON.stringify(response, null, 2);
    rawResponse.hidden = false;
    const state = response?.ambiguous ? 'warn' : 'ok';
    const truncated = response?.truncated ? ' Results were truncated.' : '';
    setStatus(`${resultCount} result${resultCount === 1 ? '' : 's'} from catalog ${response?.catalog_generation || 'unknown'}.${truncated}`, state);
}

function clearResults() {
    summary.hidden = true;
    summary.replaceChildren();
    rawResponse.hidden = true;
    rawJSON.textContent = '';
    resultList.replaceChildren(textElement('div', 'Location results will appear here.', 'location-result-empty'));
    setStatus('Ready to query the active location catalog.', 'pending');
}

async function runQuery(event) {
    event.preventDefault();
    try {
        const request = buildRequest();
        queryButton.disabled = true;
        setStatus('Querying haze-location through the event bridge...', 'pending');
        const response = await apiCommand('locations.query', request, 12000);
        renderResponse(response || {});
    } catch (error) {
        setStatus(error.message || 'Location query failed.', 'err');
    } finally {
        queryButton.disabled = false;
    }
}

function setCatalogImportStatus(text, state = 'pending') {
    if (!catalogImportStatus) return;
    catalogImportStatus.textContent = text;
    catalogImportStatus.dataset.state = state;
}

async function importLocationCatalog(event) {
    event.preventDefault();
    const file = catalogImportFile?.files?.[0] || null;
    if (!file) {
        setCatalogImportStatus('Choose the official ECCC land ZIP first.', 'err');
        return;
    }
    if (!file.name.toLowerCase().endsWith('.zip')) {
        setCatalogImportStatus('The ECCC catalog must be a ZIP archive.', 'err');
        return;
    }
    if (file.size > 96 * 1024 * 1024) {
        setCatalogImportStatus('The selected ZIP exceeds the 96 MiB upload limit.', 'err');
        return;
    }
    const body = new FormData();
    body.set('file', file, file.name);
    catalogImportButton.disabled = true;
    catalogImportFile.disabled = true;
    setCatalogImportStatus('Uploading and validating the candidate catalog...', 'pending');
    try {
        const response = await fetch('/api/v1/locations/catalog/import', {
            method: 'POST',
            credentials: 'same-origin',
            headers: session.authHeaders({ 'X-Haze-Admin-Intent': 'command' }),
            body,
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.detail || `Catalog import failed: ${response.status}`);
        const reload = payload.reload_requested ? 'haze-location reload requested.' : (payload.warning || 'haze-location reload is still required.');
        setCatalogImportStatus(`Activated ECCC CLC ${payload.provider_version || 'unknown'} with ${Number(payload.record_count || 0).toLocaleString()} areas. ${reload}`, payload.reload_requested ? 'ok' : 'warn');
        catalogImportForm.reset();
    } catch (error) {
        setCatalogImportStatus(error.message || 'Catalog import failed.', 'err');
    } finally {
        catalogImportButton.disabled = false;
        catalogImportFile.disabled = false;
    }
}

export function initUtilitiesView() {
    if (bound) return;
    bound = true;
    operation?.addEventListener('change', updateOperationFields);
    form?.addEventListener('submit', runQuery);
    catalogImportForm?.addEventListener('submit', importLocationCatalog);
    clearButton?.addEventListener('click', clearResults);
    updateOperationFields();
}
