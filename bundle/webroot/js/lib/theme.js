const THEME_KEY = 'haze.theme';

export function applyTheme(theme) {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem(THEME_KEY, theme);
}

export function initTheme(toggleEl) {
    const saved = localStorage.getItem(THEME_KEY);
    applyTheme(saved === 'light' || saved === 'dark' ? saved : 'dark');
    window.lucide?.createIcons();
    syncIcon(toggleEl);
    toggleEl?.addEventListener('click', () => {
        const cur = document.documentElement.dataset.theme;
        const isDark = cur === 'dark' || (!cur && window.matchMedia('(prefers-color-scheme: dark)').matches);
        applyTheme(isDark ? 'light' : 'dark');
        syncIcon(toggleEl);
    });
}

function syncIcon(btn) {
    if (!btn) return;
    const isDark = document.documentElement.dataset.theme === 'dark';
    const icon = btn.querySelector('[data-lucide]');
    if (icon) {
        icon.setAttribute('data-lucide', isDark ? 'sun' : 'moon');
        window.lucide?.createIcons({ nodes: [btn] });
    }
    const innerSpan = btn.querySelector('span:not([data-lucide])');
    if (innerSpan) innerSpan.textContent = isDark ? 'Light mode' : 'Dark mode';
    const labelEl = document.getElementById('themeLabel');
    if (labelEl && !btn.contains(labelEl)) labelEl.textContent = isDark ? 'Light mode' : 'Dark mode';
    btn.title = isDark ? 'Switch to light mode' : 'Switch to dark mode';
    btn.setAttribute('aria-label', btn.title);
}
