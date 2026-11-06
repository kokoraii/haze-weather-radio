//! Process-wide accounting for realtime tick lag.
//!
//! The per-feed lag warning is rate limited, so log counts understate how much
//! audio was lost. These counters see every lagged tick and are summarized once
//! a minute next to the kernel memory counters, so a stall can be tied to page
//! faults, reclaim, swap or compaction from the logs alone.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::{interval, MissedTickBehavior};

const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

static LAGGED_TICKS: AtomicU64 = AtomicU64::new(0);
static DROPPED_MS: AtomicU64 = AtomicU64::new(0);
static MAX_ELAPSED_MS: AtomicU64 = AtomicU64::new(0);
static WORKER_MAJOR_FAULTS: AtomicU64 = AtomicU64::new(0);
static READAHEAD_SKIPPED: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static SEEN_MAJOR_FAULTS: Cell<u64> = const { Cell::new(0) };
}

/// Records one lagged feed tick from the executor worker that ran it. Returns the
/// major faults this worker took since its previous lagged tick.
pub(super) fn record_tick_lag(elapsed: Duration, dropped: Duration) -> u64 {
    LAGGED_TICKS.fetch_add(1, Ordering::Relaxed);
    DROPPED_MS.fetch_add(millis(dropped), Ordering::Relaxed);
    MAX_ELAPSED_MS.fetch_max(millis(elapsed), Ordering::Relaxed);
    let faults = thread_major_faults_since_last_lag();
    WORKER_MAJOR_FAULTS.fetch_add(faults, Ordering::Relaxed);
    faults
}

/// Records a readahead request dropped because the readahead queue was full.
pub(super) fn record_readahead_skipped() {
    READAHEAD_SKIPPED.fetch_add(1, Ordering::Relaxed);
}

/// Logs one summary per interval in which something lagged. Runs for the session.
pub(super) async fn run_summary(feed_count: usize) {
    let mut ticker = interval(SUMMARY_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut previous = read_kernel_counters().await;
    loop {
        ticker.tick().await;
        let current = read_kernel_counters().await;
        let lagged_ticks = LAGGED_TICKS.swap(0, Ordering::Relaxed);
        let dropped_ms = DROPPED_MS.swap(0, Ordering::Relaxed);
        let max_elapsed_ms = MAX_ELAPSED_MS.swap(0, Ordering::Relaxed);
        let worker_major_faults = WORKER_MAJOR_FAULTS.swap(0, Ordering::Relaxed);
        let readahead_skipped = READAHEAD_SKIPPED.swap(0, Ordering::Relaxed);
        if lagged_ticks > 0 || readahead_skipped > 0 {
            tracing::warn!(
                window_s = SUMMARY_INTERVAL.as_secs(),
                lagged_ticks,
                dropped_ms,
                dropped_ms_per_feed = dropped_ms / feed_count.max(1) as u64,
                max_elapsed_ms,
                worker_major_faults,
                readahead_skipped,
                system_major_faults = current.major_faults.saturating_sub(previous.major_faults),
                pages_scanned = current.pages_scanned.saturating_sub(previous.pages_scanned),
                swap_ins = current.swap_ins.saturating_sub(previous.swap_ins),
                compaction_stalls = current
                    .compaction_stalls
                    .saturating_sub(previous.compaction_stalls),
                "playout realtime lag summary"
            );
        }
        previous = current;
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn thread_major_faults_since_last_lag() -> u64 {
    // Procfs is served from memory, so this cannot wait on the disk. Other
    // platforms have no such file and report zero.
    let Some(total) = std::fs::read_to_string("/proc/thread-self/stat")
        .ok()
        .and_then(|stat| parse_major_faults(&stat))
    else {
        return 0;
    };
    SEEN_MAJOR_FAULTS.with(|seen| total.saturating_sub(seen.replace(total)))
}

// majflt is field 12 of proc(5) stat. Fields are counted from the last ')'
// because the thread name in field 2 may itself contain spaces or parentheses.
fn parse_major_faults(stat: &str) -> Option<u64> {
    let (_, fields) = stat.rsplit_once(')')?;
    fields.split_whitespace().nth(9)?.parse().ok()
}

#[derive(Debug, Default, Eq, PartialEq)]
struct KernelCounters {
    major_faults: u64,
    pages_scanned: u64,
    swap_ins: u64,
    compaction_stalls: u64,
}

async fn read_kernel_counters() -> KernelCounters {
    tokio::fs::read_to_string("/proc/vmstat")
        .await
        .map(|vmstat| parse_vmstat(&vmstat))
        .unwrap_or_default()
}

fn parse_vmstat(vmstat: &str) -> KernelCounters {
    let mut counters = KernelCounters::default();
    for line in vmstat.lines() {
        let Some((name, value)) = line.split_once(' ') else {
            continue;
        };
        let Ok(value) = value.trim().parse::<u64>() else {
            continue;
        };
        match name {
            "pgmajfault" => counters.major_faults = value,
            "pgscan_kswapd" | "pgscan_direct" => counters.pages_scanned += value,
            "pswpin" => counters.swap_ins = value,
            "compact_stall" => counters.compaction_stalls = value,
            _ => {}
        }
    }
    counters
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn major_faults_are_read_past_a_thread_name_with_spaces_and_parens() {
        let stat = "1582 (tokio (rt) worker) S 1 2 3 4 5 6 700 8 41 10 11 12\n";

        assert_eq!(parse_major_faults(stat), Some(41));
    }

    #[test]
    fn vmstat_sums_both_reclaim_scanners_and_ignores_other_counters() {
        let vmstat = "pgmajfault 7\npgscan_kswapd 100\npgscan_direct 20\npgscan_khugepaged 5\n\
                      pswpin 3\ncompact_stall 2\ncompact_fail 9\n";

        assert_eq!(
            parse_vmstat(vmstat),
            KernelCounters {
                major_faults: 7,
                pages_scanned: 120,
                swap_ins: 3,
                compaction_stalls: 2,
            }
        );
    }
}
