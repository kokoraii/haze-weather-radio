import { session, token } from './lib/api.js';
import { initTheme } from './lib/theme.js';
import { createControlClient } from './lib/ws-client.js';
import { initDashboard } from './dashboard.js';
import { initSameView, setAccountPolicy } from './same.js?v=account-origination-20260717-1';
import { initWxView } from './wx.js';
import { initDaemonView } from './daemon.js';
import { initBreakInView } from './breakin.js';
import { initAlertsArchiveView } from './alerts.js';
import { initAutomationsView } from './automations.js';
import { initDictionaryView } from './dictionary.js';
import { initBulletinsView } from './bulletins.js';
import { initCgenView } from './cgen.js';
import { initTTSView } from './tts.js';
import { initFeedsView } from './feeds.js';
import { initAccountsView } from './accounts.js?v=accounts-20260717-2';
import { initLogsView } from './logs.js';
import { initUtilitiesView } from './utilities.js';

const authPill = document.getElementById('authPill');
const healthPill = document.getElementById('healthPill');
const apiDot = document.getElementById('apiDot');
const authDot = document.getElementById('authDot');
const themeToggle = document.getElementById('themeToggle');
const logoutButton = document.getElementById('logoutButton');
const refreshButton = document.getElementById('refreshButton');

let healthState = { auth_required: true, auth_enabled: true };
let dashboardInitialized = false;
let allowedViews = null;
const viewInit = { same: false, automations: false, wx: false, feeds: false, tts: false, daemon: false, breakin: false, cgen: false, bulletins: false, alerts: false, dictionary: false, utilities: false, accounts: false, logs: false };

session.importUrlToken();
initTheme(themeToggle);
window.lucide?.createIcons();

function setAuthState(authenticated) {
    authPill.textContent = authenticated ? 'Authenticated' : 'Required';
    authDot.dataset.state = authenticated ? 'ok' : 'warn';
}

function setHealthState(ok, text) {
    healthPill.textContent = text;
    apiDot.dataset.state = ok ? 'ok' : 'err';
}

function formatDateTime(value) {
    if (!value) return 'unknown time';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
    });
}

function setLastConnected(lastConnected) {
    const el = document.getElementById('lastConnected');
    if (!el) return;
    const ip = lastConnected?.ip || 'unknown IP';
    el.textContent = `Last connected from ${ip} at ${formatDateTime(lastConnected?.at)}`;
}

function applyAccountNavigation(account, passwordChangeRequired = false) {
	if (!account) return;
	setAccountPolicy(account);
	refreshButton.hidden = passwordChangeRequired;
	const originators = Array.isArray(account.allowed_originators) ? account.allowed_originators : [];
	const canOriginate = !passwordChangeRequired && account.allow_origination === true && originators.length > 0;
	if (passwordChangeRequired) {
		allowedViews = new Set(['accounts']);
	} else if (account.is_admin) {
		allowedViews = new Set([...document.querySelectorAll('.sidebar-link[data-view]')].map((link) => link.dataset.view));
		if (!canOriginate) allowedViews.delete('same');
	} else {
		allowedViews = new Set(['home', 'accounts']);
		if (canOriginate) allowedViews.add('same');
		if (account.can_view_logs) {
			allowedViews.add('alerts');
			allowedViews.add('logs');
		}
	}
	document.querySelectorAll('.sidebar-link[data-view]').forEach((link) => {
		link.hidden = !allowedViews.has(link.dataset.view);
	});
	document.querySelectorAll('a[href="#/same"]').forEach((link) => {
		link.hidden = !canOriginate;
	});
	const rwtButton = document.getElementById('rwtButton');
	if (rwtButton) rwtButton.hidden = !canOriginate;
	const sameView = document.querySelector('.view[data-view="same"]');
	if (sameView) sameView.hidden = !canOriginate;
}

function loginUrl() {
    return `/login?next=${encodeURIComponent(`${window.location.pathname}${window.location.hash || ''}`)}`;
}

function delay(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function readAuthState() {
    const client = createControlClient();
    try {
        healthState = await client.request('auth_check');
    } finally {
        client.close();
    }
    setHealthState(true, `API healthy · ${Math.round(healthState.uptime_seconds || 0)}s uptime`);
    setLastConnected(healthState.last_connected);
    setAuthState(Boolean(healthState.authenticated));
	applyAccountNavigation(healthState.account, Boolean(healthState.password_change_required));
	if (healthState.password_change_required && window.location.hash !== '#/accounts') {
		window.location.hash = '#/accounts';
	}
    return healthState;
}

async function loadHealth() {
    try {
        for (let attempt = 0; attempt < 3; attempt += 1) {
            const state = await readAuthState();
			const authRequired = state.auth_required ?? state.auth_enabled ?? true;
			if (!authRequired || state.authenticated) return true;
            await delay(250);
        }
		const authRequired = healthState.auth_required ?? healthState.auth_enabled ?? true;
		if (authRequired && !healthState.authenticated) {
            if (!token.get()) {
                token.clear();
                window.location.href = loginUrl();
                return false;
            }
            setHealthState(false, 'Session verification pending');
            setAuthState(false);
            return true;
        }
        return true;
    } catch {
        setHealthState(false, 'API unavailable');
        setAuthState(false);
        if (token.get()) {
            return true;
        }
        return false;
    }
}

function navigate(view) {
    document.querySelectorAll('.view').forEach((v) => {
        v.classList.toggle('active', v.dataset.view === view);
    });

    document.querySelectorAll('.sidebar-link').forEach((l) => {
        l.classList.toggle('active', l.dataset.view === view);
    });

    if (view === 'same' && !viewInit.same) {
        viewInit.same = true;
        initSameView();
    }
    if (view === 'automations' && !viewInit.automations) {
        viewInit.automations = true;
        initAutomationsView();
    }
    if (view === 'wx' && !viewInit.wx) {
        viewInit.wx = true;
        initWxView();
    }
    if (view === 'feeds' && !viewInit.feeds) {
        viewInit.feeds = true;
        initFeedsView();
    }
    if (view === 'tts' && !viewInit.tts) {
        viewInit.tts = true;
        initTTSView();
    }
    if (view === 'daemon' && !viewInit.daemon) {
        viewInit.daemon = true;
        initDaemonView();
    }
    if (view === 'breakin' && !viewInit.breakin) {
        viewInit.breakin = true;
        initBreakInView();
    }
    if (view === 'cgen' && !viewInit.cgen) {
        viewInit.cgen = true;
        initCgenView();
    }
    if (view === 'bulletins' && !viewInit.bulletins) {
        viewInit.bulletins = true;
        initBulletinsView();
    }
    if (view === 'alerts' && !viewInit.alerts) {
        viewInit.alerts = true;
        initAlertsArchiveView();
    }
    if (view === 'dictionary' && !viewInit.dictionary) {
        viewInit.dictionary = true;
        initDictionaryView();
    }
    if (view === 'utilities' && !viewInit.utilities) {
        viewInit.utilities = true;
        initUtilitiesView();
    }
    if (view === 'accounts' && !viewInit.accounts) {
        viewInit.accounts = true;
        initAccountsView();
    }
    if (view === 'logs' && !viewInit.logs) {
        viewInit.logs = true;
        initLogsView();
    }

    window.lucide?.createIcons();
}

function handleHash() {
    const hash = window.location.hash.replace('#', '') || '/';
    const requested = hash.replace(/^\//, '') || 'home';
    const link = document.querySelector(`.sidebar-link[data-view="${CSS.escape(requested)}"]`);
	const permitted = link && !link.hidden && (!allowedViews || allowedViews.has(requested));
	const fallback = allowedViews?.has('home') ? 'home' : 'accounts';
	navigate(permitted ? requested : fallback);
}

function bootstrap() {
    if (!healthState.password_change_required && !dashboardInitialized) {
        dashboardInitialized = true;
        initDashboard();
    }
    handleHash();
}

async function logout() {
    const client = createControlClient();
    try {
        return await client.request('logout', { token: token.get() }, 8000);
    } finally {
        client.close();
    }
}

logoutButton.addEventListener('click', async () => {
	let publicUrl = healthState.public_url || 'http://127.0.0.1:6444/';
	try {
		const payload = await logout();
		publicUrl = payload.public_url || publicUrl;
	} catch (error) {
		setHealthState(false, `Logout failed: ${error.message || 'session revocation was not confirmed'}`);
		return;
	}
	setAuthState(false);
	window.location.href = publicUrl;
});

window.addEventListener('haze:session-invalid', () => {
	window.location.href = loginUrl();
});

window.addEventListener('hashchange', handleHash);

refreshButton.addEventListener('click', () => {
    import('./dashboard.js').then((m) => m.refreshDashboard({ force: true })).catch(() => {});
});

window.addEventListener('haze:admin-state', (event) => {
    const payload = event.detail || {};
    const summary = payload.summary || {};
    if (typeof summary.uptime_seconds === 'number') {
        setHealthState(true, `Live stream · ${Math.round(summary.uptime_seconds)}s uptime`);
    } else {
        setHealthState(true, 'Live stream connected');
    }
    setLastConnected(payload.last_connected);
});

loadHealth().then((ready) => {
    if (ready) bootstrap();
}).catch(() => {
    const heroTitle = document.getElementById('heroTitle');
    const heroSubtitle = document.getElementById('heroSubtitle');
    if (heroTitle) heroTitle.textContent = 'Panel unavailable';
    if (heroSubtitle) heroSubtitle.textContent = 'The web API did not respond cleanly.';
});
