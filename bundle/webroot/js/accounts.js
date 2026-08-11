import { apiCommand } from './lib/api.js';

let initialized = false;
let currentProfile = null;
let accounts = [];
let selectedAccount = null;
let selectedSessions = [];
let creating = false;
let globalSecurity = {};
let selectionEpoch = 0;
let blockedEventCodes = new Set();
const nationalEventCodes = ['EAN', 'NPT', 'EAT', 'NAT', 'NIC'];
let eventCodeCatalog = new Map([
    ['ADR', 'Administrative Message'],
    ['AVA', 'Avalanche Watch'],
    ['AVW', 'Avalanche Warning'],
    ['BHW', 'Biological Hazard Warning'],
    ['BLU', 'Blue Alert'],
    ['BWW', 'Boil Water Warning'],
    ['BZW', 'Blizzard Warning'],
    ['CAE', 'Child Abduction Emergency'],
    ['CDW', 'Civil Danger Warning'],
    ['CEM', 'Civil Emergency Message'],
    ['CFA', 'Coastal Flood Watch'],
    ['CFW', 'Coastal Flood Warning'],
    ['CHW', 'Chemical Hazard Warning'],
    ['CWW', 'Contaminated Water Warning'],
    ['DBA', 'Dam Watch'],
    ['DBW', 'Dam Break Warning'],
    ['DEW', 'Disease Warning'],
    ['DMO', 'Practice/Demo Warning'],
    ['DSW', 'Dust Storm Warning'],
    ['EAN', 'Emergency Action Notification'],
    ['EAT', 'Emergency Action Termination'],
    ['EQW', 'Earthquake Warning'],
    ['EVA', 'Evacuation Watch'],
    ['EVI', 'Evacuation Immediate'],
    ['EWW', 'Extreme Wind Warning'],
    ['FCW', 'Food Contamination Warning'],
    ['FFA', 'Flash Flood Watch'],
    ['FFS', 'Flash Flood Statement'],
    ['FFW', 'Flash Flood Warning'],
    ['FLA', 'Flood Watch'],
    ['FLS', 'Flood Statement'],
    ['FLW', 'Flood Warning'],
    ['FRW', 'Fire Warning'],
    ['FSW', 'Flash Freeze Warning'],
    ['HLS', 'Hurricane Statement'],
    ['HMW', 'Hazardous Materials Warning'],
    ['HUA', 'Hurricane Watch'],
    ['HUW', 'Hurricane Warning'],
    ['HWA', 'High Wind Watch'],
    ['HWW', 'High Wind Warning'],
    ['IBW', 'Iceberg Warning'],
    ['IFW', 'Industrial Fire Warning'],
    ['LAE', 'Local Area Emergency'],
    ['LEW', 'Law Enforcement Warning'],
    ['LSW', 'Landslide Warning'],
    ['NAT', 'National Audible Test'],
    ['NIC', 'National Information Center'],
    ['NMN', 'Network Message Notification'],
    ['NPT', 'National Periodic Test'],
    ['NUW', 'Nuclear Power Plant Warning'],
    ['POS', 'Power Outage Advisory'],
    ['RHW', 'Radiological Hazard Warning'],
    ['RMT', 'Required Monthly Test'],
    ['RWT', 'Required Weekly Test'],
    ['SMW', 'Special Marine Warning'],
    ['SPS', 'Special Weather Statement'],
    ['SPW', 'Shelter in Place Warning'],
    ['SQW', 'Snow Squall Warning'],
    ['SSA', 'Storm Surge Watch'],
    ['SSW', 'Storm Surge Warning'],
    ['SVA', 'Severe Thunderstorm Watch'],
    ['SVR', 'Severe Thunderstorm Warning'],
    ['SVS', 'Severe Weather Statement'],
    ['TOA', 'Tornado Watch'],
    ['TOE', 'Telephone Outage Emergency'],
    ['TOR', 'Tornado Warning'],
    ['TRA', 'Tropical Storm Watch'],
    ['TRW', 'Tropical Storm Warning'],
    ['TSA', 'Tsunami Watch'],
    ['TSW', 'Tsunami Warning'],
    ['VOW', 'Volcano Warning'],
    ['WFA', 'Wildfire Watch'],
    ['WFW', 'Wildfire Warning'],
    ['WSA', 'Winter Storm Watch'],
    ['WSW', 'Winter Storm Warning'],
]);

const byID = (id) => document.getElementById(id);

function setStatus(message, state = '') {
    const element = byID('accountsStatus');
    if (!element) return;
    element.textContent = message;
    element.dataset.state = state;
}

function setSaveStatus(message = '', state = '') {
    const element = byID('accountSaveStatus');
    if (!element) return;
    element.textContent = message;
    element.dataset.state = state;
    element.hidden = !message;
}

function formatDate(value) {
    if (!value) return 'Never';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime()) || parsed.getUTCFullYear() <= 1) return 'Never';
	const pad = (number) => String(number).padStart(2, '0');
	return `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())} ${pad(parsed.getHours())}:${pad(parsed.getMinutes())}:${pad(parsed.getSeconds())}`;
}

function timeAgo(value) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime()) || parsed.getUTCFullYear() <= 1) return 'never';
    const seconds = Math.max(0, Math.round((Date.now() - parsed.getTime()) / 1000));
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
}

function button(label, className, onClick) {
    const element = document.createElement('button');
    element.type = 'button';
    element.className = className;
    element.textContent = label;
    element.addEventListener('click', onClick);
    return element;
}

function renderDirectory() {
    const body = byID('accountsTableBody');
    if (!body) return;
    body.replaceChildren();
    const query = byID('accountsSearch').value.trim().toLowerCase();
    const filtered = accounts.filter((account) => !query
        || account.username.toLowerCase().includes(query)
        || String(account.sender_id || '').toLowerCase().includes(query));
    for (const account of filtered) {
        const row = document.createElement('tr');
        row.classList.toggle('active', selectedAccount?.id === account.id);
        row.addEventListener('click', () => selectAccount(account.id));

        const name = document.createElement('td');
        name.textContent = account.username;
        if (account.account_locked) name.append(' (locked)');
        row.append(name);

        const sender = document.createElement('td');
        sender.textContent = account.force_sender_id && account.sender_id ? account.sender_id : 'Default';
        row.append(sender);

        const ipCell = document.createElement('td');
        if (account.last_ip) {
            const link = document.createElement('a');
            link.href = `https://ipinfo.io/${encodeURIComponent(account.last_ip)}`;
            link.target = '_blank';
            link.rel = 'noreferrer';
            link.textContent = account.last_ip;
            link.addEventListener('click', (event) => event.stopPropagation());
            ipCell.append(link);
        } else {
            ipCell.textContent = 'Unknown';
        }
        row.append(ipCell);

        const login = document.createElement('td');
		login.textContent = formatDate(account.last_login_at);
		login.title = timeAgo(account.last_login_at);
        row.append(login);
        body.append(row);
    }
    byID('accountsCount').textContent = String(accounts.length);
    byID('accountsLockedCount').textContent = String(accounts.filter((account) => account.account_locked).length);
}

function setChecked(id, value) {
    byID(id).checked = Boolean(value);
}

function setValue(id, value = '') {
    byID(id).value = value ?? '';
}

function eventCodeDescription(code) {
    return eventCodeCatalog.get(code) || 'SAME event';
}

function populateBlockedEventSelect() {
    const select = byID('accountBlockedEventSelect');
    const current = select.value;
    const available = [...eventCodeCatalog.entries()]
        .filter(([code]) => !nationalEventCodes.includes(code) && !blockedEventCodes.has(code))
        .sort(([left], [right]) => left.localeCompare(right));
    select.replaceChildren(...available.map(([code, description]) => {
        const option = document.createElement('option');
        option.value = code;
        option.textContent = `${code} - ${description}`;
        return option;
    }));
    if (available.some(([code]) => code === current)) select.value = current;
    byID('accountBlockedEventAdd').disabled = available.length === 0;
}

function renderBlockedEventCodes() {
    const nationalToggle = byID('accountBlockNationalAlerts');
    const blockedNationalCount = nationalEventCodes.filter((code) => blockedEventCodes.has(code)).length;
    nationalToggle.checked = blockedNationalCount === nationalEventCodes.length;
    nationalToggle.indeterminate = blockedNationalCount > 0 && blockedNationalCount < nationalEventCodes.length;
    const list = byID('accountBlockedEventList');
    list.replaceChildren();
    const otherBlockedCodes = [...blockedEventCodes]
        .filter((code) => !nationalEventCodes.includes(code))
        .sort();
    for (const code of otherBlockedCodes) {
        const row = document.createElement('div');
        row.className = 'accounts-blocked-event-row';
        const codeElement = document.createElement('strong');
        codeElement.textContent = code;
        const description = document.createElement('span');
        description.textContent = eventCodeDescription(code);
        const remove = button('Remove', 'btn-ghost', () => {
            blockedEventCodes.delete(code);
            renderBlockedEventCodes();
            setSaveStatus('Unsaved changes.', 'pending');
        });
        row.append(codeElement, description, remove);
        list.append(row);
    }
    if (otherBlockedCodes.length === 0) {
        const empty = document.createElement('p');
        empty.textContent = 'No other event codes are blocked.';
        list.append(empty);
    }
    populateBlockedEventSelect();
}

function setEventCodeCatalog(mapping = {}) {
    for (const [code, description] of Object.entries(mapping.eas || {})) {
        const normalized = String(code || '').trim().toUpperCase();
        if (/^[A-Z0-9]{3}$/.test(normalized)) eventCodeCatalog.set(normalized, String(description || normalized));
    }
    renderBlockedEventCodes();
}

function fillEditor(account, sessions = []) {
    selectedAccount = account;
    selectedSessions = sessions;
    creating = !account?.id;
    byID('accountsEditorTitle').textContent = creating ? 'Create account' : `Selected: ${account.username}`;
	setValue('accountID', account.id);
	setValue('accountUsername', account.username);
	byID('accountUsername').readOnly = !creating;
	byID('accountRenameButton').hidden = creating;
    setChecked('accountIsAdmin', account.is_admin);
    setChecked('accountAllowOrigination', account.allow_origination);
    for (const checkbox of document.querySelectorAll('input[name="accountOriginator"]')) {
        checkbox.checked = (account.allowed_originators || []).includes(checkbox.value);
    }
    blockedEventCodes = new Set((account.blocked_event_codes || []).map((code) => String(code).trim().toUpperCase()).filter(Boolean));
    renderBlockedEventCodes();
    setChecked('accountForceOriginatorName', account.force_originator_name);
    setValue('accountOriginatorName', account.originator_name_text);
    setChecked('accountIncludeIP', account.include_ip_in_brackets);
    setChecked('accountForceSenderID', account.force_sender_id);
    setValue('accountSenderID', account.sender_id);
    setChecked('accountAllowPasswordChange', account.allow_user_pw_change ?? true);
    setValue('accountPasswordExpiry', account.password_expiry_days ?? 90);
    setChecked('accountAllowPersistent', account.allow_persistent_sessions);
    setChecked('accountLoggingEnabled', account.logging_enabled ?? true);
    setChecked('accountCanViewLogs', account.can_view_logs);
    setValue('accountCIDRAllowlist', (account.cidr_allowlist || []).join('\n'));
    setValue('accountDeleteConfirmation', '');
    setValue('accountInitialPassword', '');
    byID('accountInitialPasswordRow').hidden = !creating;
    byID('accountInitialPasswordLabel').textContent = creating ? 'Initial password' : 'New password';
    byID('accountPasswordResetButton').textContent = 'Change password';
    byID('accountPasswordResetButton').hidden = creating;
    byID('accountMFAResetButton').hidden = creating || !account.mfa_configured;
    byID('accountUnlockButton').hidden = creating || !account.account_locked;
    byID('accountRevokeAllButton').disabled = creating || sessions.length === 0;
    byID('accountDeleteButton').disabled = creating;
    byID('accountSecurityState').replaceChildren(
        securityPill(account.mfa_enabled ? 'MFA active' : account.mfa_configured ? 'MFA enrollment pending' : 'MFA not enrolled'),
        securityPill(account.account_locked ? 'Account locked' : 'Account unlocked'),
        securityPill(`Password changed ${timeAgo(account.password_changed_at)}`),
    );
    renderSessions();
    updateConditionalFields();
    renderDirectory();
    setSaveStatus();
}

function securityPill(text) {
    const element = document.createElement('span');
    element.textContent = text;
    return element;
}

function renderSessions() {
    const list = byID('accountSessions');
    list.replaceChildren();
    if (selectedSessions.length === 0) {
        const empty = document.createElement('p');
        empty.textContent = 'No active sessions.';
        list.append(empty);
        return;
    }
    for (const session of selectedSessions) {
        const item = document.createElement('div');
        item.className = 'accounts-session-item';
        const summary = document.createElement('div');
        const title = document.createElement('strong');
        title.textContent = session.user_agent || 'Unknown client';
        const metadata = document.createElement('p');
        metadata.textContent = `${session.ip || 'unknown IP'} · active ${timeAgo(session.last_seen_at)} · expires ${formatDate(session.expires_at)}`;
        summary.append(title, metadata);
        item.append(summary, button('Revoke', 'btn-danger', () => revokeSession(session.id)));
        list.append(item);
    }
}

function updateConditionalFields() {
    const allowed = byID('accountAllowOrigination').checked;
    byID('accountOriginationOptions').hidden = !allowed;
    const forceName = byID('accountForceOriginatorName').checked;
    byID('accountOriginatorNameRow').hidden = !forceName;
    byID('accountIncludeIPRow').hidden = !forceName;
    byID('accountSenderIDRow').hidden = !byID('accountForceSenderID').checked;
}

function editorPayload() {
    return {
        id: byID('accountID').value,
        username: byID('accountUsername').value.trim(),
        password: byID('accountInitialPassword').value,
        is_admin: byID('accountIsAdmin').checked,
        allow_origination: byID('accountAllowOrigination').checked,
        allowed_originators: [...document.querySelectorAll('input[name="accountOriginator"]:checked')].map((input) => input.value),
        blocked_event_codes: [...blockedEventCodes].sort(),
        force_originator_name: byID('accountForceOriginatorName').checked,
        originator_name_text: byID('accountOriginatorName').value.trim(),
        include_ip_in_brackets: byID('accountIncludeIP').checked,
        force_sender_id: byID('accountForceSenderID').checked,
        sender_id: byID('accountSenderID').value.trim().toUpperCase(),
        allow_user_pw_change: byID('accountAllowPasswordChange').checked,
        password_expiry_days: Number(byID('accountPasswordExpiry').value || 0),
        allow_persistent_sessions: byID('accountAllowPersistent').checked,
        logging_enabled: byID('accountLoggingEnabled').checked,
        can_view_logs: byID('accountCanViewLogs').checked,
        cidr_allowlist: byID('accountCIDRAllowlist').value.split(/[\n,]+/).map((value) => value.trim()).filter(Boolean),
    };
}

function durationLabel(seconds) {
	const value = Number(seconds || 0);
	if (!value) return 'Disabled';
	if (value % 86400 === 0) return `${value / 86400} days`;
	if (value % 3600 === 0) return `${value / 3600} hours`;
	if (value % 60 === 0) return `${value / 60} minutes`;
	return `${value} seconds`;
}

function renderGlobalSecurity() {
	setChecked('accountsGlobalMFA', globalSecurity.enforce_mfa);
	setChecked('accountsGlobalRedis', globalSecurity.redis_required);
	setValue('accountsGlobalSessionTTL', durationLabel(globalSecurity.session_ttl_seconds));
	setValue('accountsGlobalPersistentTTL', durationLabel(globalSecurity.persistent_session_ttl_seconds));
	setValue('accountsGlobalIdleTTL', durationLabel(globalSecurity.idle_timeout_seconds));
	const loginWindow = durationLabel(globalSecurity.login_rate_window_seconds || 120);
	const lockDuration = durationLabel(globalSecurity.account_lock_duration_seconds || 120);
	const adminPolicy = globalSecurity.administrator_lock_exempt ? '; administrators exempt' : '';
	setValue('accountsGlobalLoginLimit', `${globalSecurity.login_rate_limit || 5} in ${loginWindow}; ${lockDuration} lock${adminPolicy}`);
	setValue('accountsGlobalOriginationRate', `${globalSecurity.origination_rate_per_second || 2} requests per second`);
	setValue('accountsGlobalCIDRs', (globalSecurity.login_cidr_allowlist || []).join('\n') || 'Any source, then apply each account allowlist');
}

async function refreshAccounts({ selectID = selectedAccount?.id } = {}) {
	const payload = await apiCommand('accounts.list');
	accounts = payload.accounts || [];
	globalSecurity = payload.security || {};
	renderGlobalSecurity();
    renderDirectory();
    if (selectID && accounts.some((account) => account.id === selectID)) {
        await selectAccount(selectID);
    } else if (accounts.length > 0) {
        await selectAccount(accounts[0].id);
    }
}

async function selectAccount(id) {
    const requestEpoch = ++selectionEpoch;
    try {
        setStatus('Loading account policy...', 'pending');
        const payload = await apiCommand('accounts.get', { id });
        if (requestEpoch !== selectionEpoch) return;
        fillEditor(payload.account, payload.sessions || []);
        setStatus(`Editing ${payload.account.username}.`, 'ok');
    } catch (error) {
        if (requestEpoch !== selectionEpoch) return;
        setStatus(error.message || 'Unable to load account.', 'err');
    }
}

async function saveAccount() {
    const payload = editorPayload();
    if (!payload.username) {
        setStatus('Username is required.', 'err');
        return;
    }
    if (creating && payload.password.length < 12) {
        setStatus('The initial password must contain at least 12 characters.', 'err');
        return;
    }
    const saveButton = byID('accountSaveButton');
    const savingSelf = !creating && payload.id === currentProfile?.account?.id;
    try {
        setStatus(creating ? 'Creating account...' : 'Saving account policy...', 'pending');
        setSaveStatus('Saving...', 'pending');
        saveButton.disabled = true;
        const result = await apiCommand(creating ? 'accounts.create' : 'accounts.update', payload);
        if (savingSelf) {
            setStatus('Account policy saved. Sign in again to continue.', 'ok');
            setSaveStatus('Saved. Sign in again.', 'ok');
            window.setTimeout(() => window.location.assign('/login.html'), 1200);
            return;
        }
        await refreshAccounts({ selectID: result.account.id });
        setStatus('Account policy saved.', 'ok');
        setSaveStatus('Saved.', 'ok');
    } catch (error) {
        const message = error.message || 'Unable to save account.';
        setStatus(message, 'err');
        setSaveStatus(message, 'err');
    } finally {
        saveButton.disabled = false;
    }
}

function beginCreate() {
    selectionEpoch += 1;
    fillEditor({
        id: '', username: '', allowed_originators: [], password_expiry_days: 90,
        allow_user_pw_change: true, logging_enabled: true,
    }, []);
    byID('accountUsername').focus();
    setStatus('Enter the new account policy and an initial password.', 'pending');
}

function beginRename() {
	if (!selectedAccount?.id) return;
	const next = window.prompt('Enter the new account username:', byID('accountUsername').value);
	if (next === null) return;
	const username = next.trim();
	if (!/^[A-Za-z0-9_.-]{1,50}$/.test(username)) {
		setStatus('Username must be 1 to 50 letters, numbers, periods, underscores, or hyphens.', 'err');
		return;
	}
	setValue('accountUsername', username);
	setStatus('Rename staged. Save the account to apply it and migrate its audit logs.', 'pending');
	setSaveStatus('Unsaved rename.', 'pending');
}

async function resetPassword() {
	const targetID = selectedAccount?.id;
	if (!targetID) return;
    const row = byID('accountInitialPasswordRow');
    const password = byID('accountInitialPassword').value;
    if (row.hidden) {
        row.hidden = false;
        byID('accountInitialPasswordLabel').textContent = 'New password';
        byID('accountPasswordResetButton').textContent = 'Apply password';
        byID('accountInitialPassword').focus();
        return;
    }
    if (password.length < 12) {
        setStatus('The new password must contain at least 12 characters.', 'err');
        return;
    }
    try {
		await apiCommand('accounts.password.reset', { id: targetID, password });
        row.hidden = true;
        setValue('accountInitialPassword', '');
        byID('accountPasswordResetButton').textContent = 'Change password';
		await selectAccount(targetID);
        setStatus('Password changed and all sessions revoked.', 'ok');
    } catch (error) {
        setStatus(error.message || 'Unable to change password.', 'err');
    }
}

async function simpleAccountCommand(command, successMessage) {
	const targetID = selectedAccount?.id;
	if (!targetID) return;
    try {
		await apiCommand(command, { id: targetID });
		await selectAccount(targetID);
        setStatus(successMessage, 'ok');
    } catch (error) {
        setStatus(error.message || 'Account action failed.', 'err');
    }
}

async function revokeSession(sessionID) {
	const targetID = selectedAccount?.id;
	if (!targetID) return;
    try {
		await apiCommand('accounts.sessions.revoke', { id: targetID, session_id: sessionID });
		await selectAccount(targetID);
        setStatus('Session revoked.', 'ok');
    } catch (error) {
        setStatus(error.message || 'Unable to revoke session.', 'err');
    }
}

async function deleteAccount() {
	const targetID = selectedAccount?.id;
	if (!targetID) return;
    const confirmation = byID('accountDeleteConfirmation').value;
    try {
		await apiCommand('accounts.delete', { id: targetID, confirmation });
        selectedAccount = null;
        await refreshAccounts({ selectID: currentProfile?.account?.id });
        setStatus('Account deleted and its sessions revoked.', 'ok');
    } catch (error) {
        setStatus(error.message || 'Unable to delete account.', 'err');
    }
}

async function changeOwnPassword() {
    const current = byID('profileCurrentPassword').value;
    const next = byID('profileNewPassword').value;
    const confirm = byID('profileConfirmPassword').value;
    if (next !== confirm) {
        setStatus('The new passwords do not match.', 'err');
        return;
    }
    if (next.length < 12) {
        setStatus('The new password must contain at least 12 characters.', 'err');
        return;
    }
    try {
        await apiCommand('profile.password.change', { current_password: current, new_password: next });
        setStatus('Password changed. Sign in again with the new password.', 'ok');
        window.setTimeout(() => { window.location.href = '/login?next=%2Fadmin%23%2Faccounts'; }, 900);
    } catch (error) {
        setStatus(error.message || 'Unable to change password.', 'err');
    }
}

function bindEvents() {
    byID('accountsSearch').addEventListener('input', renderDirectory);
	byID('accountsCreateButton').addEventListener('click', beginCreate);
	byID('accountRenameButton').addEventListener('click', beginRename);
    byID('accountSaveButton').addEventListener('click', saveAccount);
    byID('accountPasswordResetButton').addEventListener('click', resetPassword);
    byID('accountMFAResetButton').addEventListener('click', () => simpleAccountCommand('accounts.mfa.reset', 'MFA enrollment reset and sessions revoked.'));
    byID('accountUnlockButton').addEventListener('click', () => simpleAccountCommand('accounts.unlock', 'Account unlocked.'));
    byID('accountRevokeAllButton').addEventListener('click', () => simpleAccountCommand('accounts.sessions.revoke_all', 'All account sessions revoked.'));
    byID('accountDeleteButton').addEventListener('click', deleteAccount);
    byID('profileChangePasswordButton').addEventListener('click', changeOwnPassword);
    for (const id of ['accountAllowOrigination', 'accountForceOriginatorName', 'accountForceSenderID']) {
        byID(id).addEventListener('change', updateConditionalFields);
    }
    byID('accountSenderID').addEventListener('input', () => {
        byID('accountSenderID').value = byID('accountSenderID').value
            .replace(/-/g, '/')
            .replace(/[^A-Za-z0-9/]/g, '')
            .toUpperCase()
            .slice(0, 8);
    });
    byID('accountBlockNationalAlerts').addEventListener('change', (event) => {
        for (const code of nationalEventCodes) {
            if (event.currentTarget.checked) blockedEventCodes.add(code);
            else blockedEventCodes.delete(code);
        }
        renderBlockedEventCodes();
        setSaveStatus('Unsaved changes.', 'pending');
    });
    byID('accountBlockedEventAdd').addEventListener('click', () => {
        const code = byID('accountBlockedEventSelect').value;
        if (!code) return;
        blockedEventCodes.add(code);
        renderBlockedEventCodes();
        setSaveStatus('Unsaved changes.', 'pending');
    });
	byID('accountsEditor').addEventListener('input', () => setSaveStatus('Unsaved changes.', 'pending'));
	byID('accountsEditor').addEventListener('change', () => setSaveStatus('Unsaved changes.', 'pending'));
}

export async function initAccountsView() {
    if (initialized) return;
    initialized = true;
    bindEvents();
    try {
		currentProfile = await apiCommand('profile.get');
		const isAdmin = Boolean(currentProfile.account?.is_admin);
		const passwordChangeRequired = Boolean(currentProfile.password_change_required);
		const mayChangePassword = Boolean(currentProfile.account?.allow_user_pw_change);
		byID('accountsProfilePassword').hidden = !mayChangePassword;
		byID('accountsAdminSurface').hidden = !isAdmin || passwordChangeRequired;
		byID('accountsAdminMetrics').hidden = !isAdmin || passwordChangeRequired;
		if (passwordChangeRequired && !mayChangePassword) {
			setStatus('Your password has expired and must be reset by another administrator.', 'err');
		} else if (passwordChangeRequired) {
			setStatus('Change your expired password before using the panel.', 'err');
		} else if (isAdmin) {
            const mapping = await apiCommand('same.event_codes').catch(() => ({}));
            setEventCodeCatalog(mapping);
            await refreshAccounts({ selectID: currentProfile.account.id });
            setStatus('Account security policies loaded.', 'ok');
        } else {
            setStatus(`Signed in as ${currentProfile.account.username}.`, 'ok');
        }
    } catch (error) {
        setStatus(error.message || 'Unable to load account security state.', 'err');
    }
}
