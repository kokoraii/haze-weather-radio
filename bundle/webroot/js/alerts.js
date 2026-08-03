import { panelClient } from './lib/ws-client.js';

const statusBanner = document.getElementById('alertsArchiveStatus');
const list = document.getElementById('alertsArchiveList');
const acceptedMetric = document.getElementById('alertsAcceptedMetric');
const expiredMetric = document.getElementById('alertsExpiredMetric');
const clearExpiredButton = document.getElementById('alertsClearExpired');
const clearAllButton = document.getElementById('alertsClearAll');
const expireAllButton = document.getElementById('alertsExpireAll');
const feedSelect = document.getElementById('alertsArchiveFeedSelect');

let bound = false;
let activeTab = 'accepted';
let activeFeedFilter = '';
let archiveShiftDown = false;
let archiveState = {
    accepted_by_feed: [],
    rejected: [],
    expired: [],
};

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (char) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
    }[char]));
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

function setStatus(text, state = 'ok') {
    statusBanner.textContent = text;
    statusBanner.dataset.state = state;
}

function recordsForTab(tab) {
    if (tab === 'accepted') return Array.isArray(archiveState.accepted_by_feed) ? archiveState.accepted_by_feed : [];
    if (tab === 'rejected') return Array.isArray(archiveState.rejected) ? archiveState.rejected : [];
    return Array.isArray(archiveState.expired) ? archiveState.expired : [];
}

function archiveFeedIDs() {
    const seen = new Set();
    for (const tab of ['accepted', 'rejected', 'expired']) {
        for (const record of recordsForTab(tab)) {
            const feedID = record.feed_id || 'unassigned';
            if (feedID) seen.add(feedID);
        }
    }
    return [...seen].sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }));
}

function renderFeedSelect() {
    if (!feedSelect) return;
    const feeds = archiveFeedIDs();
    if (activeFeedFilter && !feeds.includes(activeFeedFilter)) {
        activeFeedFilter = '';
    }
    feedSelect.innerHTML = [
        '<option value="">All feeds</option>',
        ...feeds.map((feedID) => `<option value="${escapeHtml(feedID)}">${escapeHtml(feedID)}</option>`),
    ].join('');
    feedSelect.value = activeFeedFilter;
    feedSelect.disabled = feeds.length === 0;
}

function filterRecordsByFeed(records) {
    if (!activeFeedFilter) return records;
    return records.filter((record) => (record.feed_id || 'unassigned') === activeFeedFilter);
}

function groupedAccepted(records) {
    const groups = new Map();
    for (const record of records) {
        const feedID = record.feed_id || 'unassigned';
        if (!groups.has(feedID)) groups.set(feedID, []);
        groups.get(feedID).push(record);
    }
    return [...groups.entries()];
}

function metaItems(record) {
    const items = [
        ['Event', record.event || 'unknown'],
        ['Feed', record.feed_id || 'none'],
        ['Status', record.status || record.bucket || 'unknown'],
        ['Message', record.message_type || 'unknown'],
        ['Severity', record.severity || 'unknown'],
        ['Urgency', record.urgency || 'unknown'],
        ['Certainty', record.certainty || 'unknown'],
        ['Sent', formatDateTime(record.sent)],
        ['Expires', formatDateTime(record.expires)],
    ];
    if (record.reason) items.splice(3, 0, ['Reason', record.reason]);
    if (record.audio_url) items.push(['Audio', record.audio_mime_type || 'available']);
    return items;
}

function findArchiveRecord(id, feedID) {
    const records = [
        ...recordsForTab('accepted'),
        ...recordsForTab('rejected'),
        ...recordsForTab('expired'),
    ];
    return records.find((record) => {
        if ((record.id || '') !== id) return false;
        return !feedID || (record.feed_id || '') === feedID;
    }) || null;
}

function archiveRecordForCard(card) {
    if (!card) return null;
    return findArchiveRecord(card.dataset.alertId || '', card.dataset.feedId || '');
}

function releasePreviewURL(audio) {
    const url = audio?.dataset?.objectUrl || '';
    if (url) {
        URL.revokeObjectURL(url);
        delete audio.dataset.objectUrl;
    }
}

function previewPlayer(card) {
    const panel = card?.querySelector('.alert-preview-panel');
    const audio = panel?.querySelector('audio');
    if (!panel || !audio) return null;
    panel.hidden = false;
    return audio;
}

function base64ToBlob(value, type) {
    const binary = atob(value || '');
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
        bytes[index] = binary.charCodeAt(index);
    }
    return new Blob([bytes], { type });
}

function archiveActionButtonLabel(button, shiftDown = archiveShiftDown) {
    if (!button) return '';
    if (button.dataset.confirming === '1') return button.textContent || '';
    return shiftDown && button.dataset.shiftLabel ? button.dataset.shiftLabel : button.dataset.label || button.textContent || '';
}

function renderArchiveActionButton(button, shiftDown = archiveShiftDown) {
    if (!button || button.dataset.confirming === '1') return;
    const icon = button.dataset.icon || 'radio';
    const label = archiveActionButtonLabel(button, shiftDown);
    button.innerHTML = `<i data-lucide="${escapeHtml(icon)}" width="13" height="13"></i>${escapeHtml(label)}`;
}

function updateArchiveShiftLabels(shiftDown) {
    archiveShiftDown = shiftDown;
    document.querySelectorAll('[data-shift-label]').forEach((button) => renderArchiveActionButton(button, shiftDown));
    window.lucide?.createIcons();
}

function alertCard(record) {
    const id = record.id || '';
    const feedID = record.feed_id || '';
    const headline = record.headline || record.event || 'Weather Alert';
    const areas = Array.isArray(record.areas) ? record.areas.join('; ') : '';
    const broadcastLabel = record.broadcast_action_label || (record.relayed ? 'Rebroadcast' : 'Force Broadcast');
    const broadcastNoSameLabel = `${broadcastLabel} (No SAME)`;
    const broadcastAction = record.relayed ? 'rebroadcast' : 'force_broadcast';
    const broadcastNoSameAction = record.relayed ? 'rebroadcast_without_same' : 'force_broadcast_without_same';
    const identifier = record.cap_xml_url
        ? `<a class="alert-card-id" href="${escapeHtml(record.cap_xml_url)}" target="_blank" rel="noopener noreferrer" title="Open CAP XML">${escapeHtml(id)}</a>`
        : `<span class="alert-card-id">${escapeHtml(id)}</span>`;
    const meta = metaItems(record).map(([key, value]) => `
        <span><b>${escapeHtml(key)}</b>${escapeHtml(value)}</span>
    `).join('');
    const capAudioButton = record.audio_url ? `
                <button class="btn-action" type="button" data-archive-action="preview_cap_audio">
                    <i data-lucide="circle-play" width="13" height="13"></i>
                    CAP Audio
                </button>` : '';
    const samePreviewButton = record.same_preview_available ? `
                <button class="btn-action" type="button" data-archive-action="preview_same_audio">
                    <i data-lucide="audio-lines" width="13" height="13"></i>
                    SAME Preview
                </button>` : '';
    return `
        <article class="alert-card" data-alert-id="${escapeHtml(id)}" data-feed-id="${escapeHtml(feedID)}">
            <div class="alert-card-main">
                <div class="alert-card-head">
                    <div>
                        <h3><span>${escapeHtml(headline)}</span>${identifier}</h3>
                        <p>${escapeHtml(areas || record.sender || 'No area text available')}</p>
                    </div>
                    <span class="alert-card-time">${escapeHtml(formatDateTime(record.updated_at || record.sent))}</span>
                </div>
                <div class="alert-card-meta">${meta}</div>
                <details class="alert-details">
                    <summary>Details</summary>
                    <div class="alert-details-grid">
                        <section>
                            <h4>Description</h4>
                            <p>${escapeHtml(record.description || 'No description provided.')}</p>
                        </section>
                        <section>
                            <h4>Instruction</h4>
                            <p>${escapeHtml(record.instruction || 'No instruction provided.')}</p>
                        </section>
                    </div>
                </details>
                <div class="alert-preview-panel" hidden>
                    <audio class="alert-preview-player" controls preload="none"></audio>
                </div>
            </div>
            <div class="alert-card-actions">
                ${capAudioButton}
                ${samePreviewButton}
                <button class="btn-action" type="button" data-archive-action="${escapeHtml(broadcastAction)}" data-archive-action-shift="${escapeHtml(broadcastNoSameAction)}" data-label="${escapeHtml(broadcastLabel)}" data-shift-label="${escapeHtml(broadcastNoSameLabel)}" data-icon="radio">
                    <i data-lucide="radio" width="13" height="13"></i>
                    ${escapeHtml(broadcastLabel)}
                </button>
                <button class="btn-danger" type="button" data-archive-action="delete">
                    <i data-lucide="trash-2" width="13" height="13"></i>
                    Delete
                </button>
            </div>
        </article>
    `;
}

function renderAccepted(records) {
    const groups = groupedAccepted(records);
    if (!groups.length) return '<article class="alert-empty">No accepted alerts are active for any feed.</article>';
    return groups.map(([feedID, items]) => `
        <section class="alert-feed-group">
            <div class="alert-feed-group-hd">
                <strong>${escapeHtml(feedID)}</strong>
                <span>${items.length} active</span>
            </div>
            ${items.map(alertCard).join('')}
        </section>
    `).join('');
}

function renderArchive() {
    renderFeedSelect();
    const acceptedAll = recordsForTab('accepted');
    const expiredAll = recordsForTab('expired');
    const accepted = filterRecordsByFeed(acceptedAll);
    const expired = filterRecordsByFeed(expiredAll);
    acceptedMetric.textContent = activeFeedFilter ? `${accepted.length}/${acceptedAll.length}` : String(acceptedAll.length);
    expiredMetric.textContent = activeFeedFilter ? `${expired.length}/${expiredAll.length}` : String(expiredAll.length);

    const records = filterRecordsByFeed(recordsForTab(activeTab));
    if (activeTab === 'accepted') {
        list.innerHTML = renderAccepted(records);
    } else {
        list.innerHTML = records.length
            ? records.map(alertCard).join('')
            : `<article class="alert-empty">No ${escapeHtml(activeTab)} alerts are archived.</article>`;
    }
    window.lucide?.createIcons();
    updateArchiveShiftLabels(archiveShiftDown);
}

async function loadArchive() {
    setStatus('Loading alert archive...', 'pending');
    archiveState = await panelClient.command('alerts.archive.get', {}, 12000);
    renderArchive();
    setStatus('Alert archive loaded.', 'ok');
}

async function runAction(action, payload = {}) {
    setStatus('Sending alert archive command...', 'pending');
    await panelClient.command('alerts.archive.action', { action, ...payload }, 20000);
    await loadArchive();
}

async function previewCAPAudio(card) {
    const record = archiveRecordForCard(card);
    if (!record?.audio_url) {
        setStatus('This archived alert does not include CAP audio.', 'err');
        return;
    }
    const audio = previewPlayer(card);
    if (!audio) return;
    releasePreviewURL(audio);
    audio.onended = null;
    audio.src = record.audio_url;
    audio.load();
    setStatus('Previewing CAP audio.', 'ok');
    try {
        await audio.play();
    } catch {
        setStatus('CAP audio is ready. Press play in the preview control.', 'ok');
    }
}

async function previewSAMEAudio(card) {
    const record = archiveRecordForCard(card);
    if (!record) return;
    const audio = previewPlayer(card);
    if (!audio) return;
    setStatus('Generating SAME preview...', 'pending');
    const result = await panelClient.command('alerts.archive.action', {
        action: 'preview_same',
        id: record.id || '',
        feed_id: record.feed_id || '',
    }, 25000);
    const blob = base64ToBlob(result.same_audio_wav_base64, result.same_audio_mime_type || 'audio/wav');
    const objectURL = URL.createObjectURL(blob);
    releasePreviewURL(audio);
    audio.dataset.objectUrl = objectURL;
    audio.onended = null;
    audio.src = objectURL;
    if (result.audio_url) {
        audio.onended = async () => {
            releasePreviewURL(audio);
            audio.onended = null;
            audio.src = result.audio_url;
            try {
                await audio.play();
            } catch {
                setStatus('SAME finished. CAP audio is ready in the preview control.', 'ok');
            }
        };
    }
    audio.load();
    setStatus(result.audio_url ? 'Previewing SAME, then CAP audio.' : 'Previewing SAME audio.', 'ok');
    try {
        await audio.play();
    } catch {
        setStatus('SAME preview is ready. Press play in the preview control.', 'ok');
    }
}

function makeDoubleClickConfirm(button, callback) {
    let armed = false;
    let timer = null;
    const original = button.innerHTML;
    button.addEventListener('click', () => {
        if (!armed) {
            armed = true;
            button.classList.add('is-confirming');
            button.textContent = 'Click again to confirm';
            timer = window.setTimeout(() => {
                armed = false;
                button.classList.remove('is-confirming');
                button.innerHTML = original;
                window.lucide?.createIcons();
            }, 2600);
            return;
        }
        window.clearTimeout(timer);
        armed = false;
        button.classList.remove('is-confirming');
        button.disabled = true;
        callback().catch((error) => {
            setStatus(error.message || 'Alert archive command failed.', 'err');
        }).finally(() => {
            button.disabled = false;
            button.innerHTML = original;
            window.lucide?.createIcons();
        });
    });
}

function bindActionDelegates() {
    list.addEventListener('click', (event) => {
        const button = event.target.closest('[data-archive-action]');
        if (!button) return;
        const card = button.closest('.alert-card');
        const shiftedAction = button.dataset.archiveActionShift || '';
        const action = event.shiftKey && shiftedAction ? shiftedAction : button.dataset.archiveAction;
        const id = card?.dataset.alertId || '';
        const feedID = card?.dataset.feedId || '';
        if (!id) return;
        if (action === 'preview_cap_audio') {
            previewCAPAudio(card).catch((error) => setStatus(error.message || 'CAP audio preview failed.', 'err'));
            return;
        }
        if (action === 'preview_same_audio') {
            button.disabled = true;
            previewSAMEAudio(card).catch((error) => {
                setStatus(error.message || 'SAME preview failed.', 'err');
            }).finally(() => {
                button.disabled = false;
                window.lucide?.createIcons();
            });
            return;
        }
        if (button.dataset.confirming === '1') {
            const pendingAction = button.dataset.pendingAction || action;
            button.dataset.confirming = '0';
            button.disabled = true;
            runAction(pendingAction, { id, feed_id: feedID }).catch((error) => {
                setStatus(error.message || 'Alert archive command failed.', 'err');
            }).finally(() => {
                button.disabled = false;
                delete button.dataset.confirming;
                delete button.dataset.pendingAction;
                renderArchive();
            });
            return;
        }
        button.dataset.confirming = '1';
        button.dataset.pendingAction = action;
        const original = button.innerHTML;
        button.dataset.originalHtml = original;
        button.textContent = 'Click again';
        window.setTimeout(() => {
            if (button.dataset.confirming === '1') {
                button.dataset.confirming = '0';
                delete button.dataset.pendingAction;
                button.innerHTML = button.dataset.originalHtml || original;
                renderArchiveActionButton(button);
                window.lucide?.createIcons();
            }
        }, 2600);
    });
}

export function initAlertsArchiveView() {
    if (bound) {
        loadArchive().catch((error) => setStatus(error.message || 'Unable to load alert archive.', 'err'));
        return;
    }
    bound = true;
    document.querySelectorAll('[data-alert-tab]').forEach((button) => {
        button.addEventListener('click', () => {
            activeTab = button.dataset.alertTab || 'accepted';
            document.querySelectorAll('[data-alert-tab]').forEach((tab) => {
                const active = tab === button;
                tab.classList.toggle('active', active);
                tab.setAttribute('aria-selected', active ? 'true' : 'false');
                tab.tabIndex = active ? 0 : -1;
            });
            renderArchive();
        });
    });
    feedSelect?.addEventListener('change', () => {
        activeFeedFilter = feedSelect.value || '';
        renderArchive();
    });
    bindActionDelegates();
    window.addEventListener('keydown', (event) => {
        if (event.key === 'Shift') updateArchiveShiftLabels(true);
    });
    window.addEventListener('keyup', (event) => {
        if (event.key === 'Shift') updateArchiveShiftLabels(false);
    });
    window.addEventListener('blur', () => updateArchiveShiftLabels(false));
    makeDoubleClickConfirm(clearExpiredButton, () => runAction('clear_expired'));
    makeDoubleClickConfirm(clearAllButton, () => runAction('clear_all'));
    makeDoubleClickConfirm(expireAllButton, () => runAction('expire_all'));
    window.addEventListener('haze:admin-state', () => {
        if (document.querySelector('.view[data-view="alerts"]')?.classList.contains('active')) {
            loadArchive().catch(() => {});
        }
    });
    loadArchive().catch((error) => setStatus(error.message || 'Unable to load alert archive.', 'err'));
}
