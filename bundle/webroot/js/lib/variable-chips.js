// Placeholders the backend expands per feed (see services/go/internal/alertvars).
export const ALERT_VARIABLES = [
    'feed-id', 'eventType', 'transmitterCall', 'transmitterCall[1]', 'primaryCoverage',
    'networkName', 'operatorName', 'phoneNumber', 'autoHour', 'autoMin', 'autoAmPm',
    'autoDay', 'timeZone',
];

// Fills `bar` with one button per variable. Clicking inserts the token at the
// cursor of the textarea returned by getTarget().
export function bindVariableChips(bar, getTarget) {
    bar.insertAdjacentHTML('beforeend', ALERT_VARIABLES
        .map((name) => `<button type="button" data-var="{${name}}">{${name}}</button>`).join(''));
    bar.addEventListener('click', (event) => {
        const token = event.target.closest('button[data-var]')?.dataset.var;
        const target = getTarget();
        if (!token || !target) return;
        const { selectionStart: start, selectionEnd: end, value } = target;
        target.value = value.slice(0, start) + token + value.slice(end);
        target.focus();
        target.setSelectionRange(start + token.length, start + token.length);
        target.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
