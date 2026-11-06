//! Pages file-backed PCM into memory before the realtime tick needs it.
//!
//! Feed ticks copy PCM straight out of mmaps on the two executor workers, so a
//! page evicted under memory pressure would block a worker on the disk and stall
//! every feed scheduled on it. A dedicated thread takes those faults instead:
//! each feed keeps asking it to touch the next few seconds of whatever it is about
//! to play, without ever waiting on it from a tick. Heap-backed PCM needs nothing.

use std::collections::HashMap;
use std::hint::black_box;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::Instant;

use super::{lag_stats, AudioItem, SegmentGroupPlayback, SharedPcm};

/// How much upcoming audio each feed keeps paged in. The refresh interval leaves
/// about 2.5 s of margin, which is longer than the worst observed disk stall.
const READAHEAD_LEAD_MS: usize = 3_000;
const READAHEAD_REFRESH: Duration = Duration::from_millis(500);
/// Bounded so a stuck disk costs skipped requests, not memory or a blocked tick.
/// Holds about 1.7 s of refreshes for 76 feeds.
const READAHEAD_QUEUE_CAPACITY: usize = 256;
/// An item waits at most this long for its first seconds to be paged in. Past that
/// it starts anyway, since a late start is worse than risking one worker fault.
const START_WAIT: Duration = Duration::from_millis(250);
const PAGE_STRIDE: usize = 4096;

struct ReadaheadJob {
    ranges: Vec<(SharedPcm, Range<usize>)>,
    done: Option<Arc<AtomicBool>>,
}

/// Handle to the process-wide readahead thread, which exits with its last handle.
#[derive(Clone)]
pub(super) struct PcmReadahead {
    jobs: SyncSender<ReadaheadJob>,
}

impl PcmReadahead {
    pub(super) fn spawn() -> Result<Self> {
        let (jobs, requests) = sync_channel::<ReadaheadJob>(READAHEAD_QUEUE_CAPACITY);
        thread::Builder::new()
            .name("haze-pcm-readahead".to_string())
            .spawn(move || {
                // Jobs hold PCM references, so a mapping whose last user was a job
                // is also unmapped here instead of on an executor worker.
                while let Ok(job) = requests.recv() {
                    for (pcm, range) in &job.ranges {
                        touch_pages(&pcm[range.clone()]);
                    }
                    if let Some(done) = job.done {
                        done.store(true, Ordering::Release);
                    }
                }
            })
            .context("failed to start PCM readahead thread")?;
        Ok(Self { jobs })
    }

    // Never blocks. Returns false if the job was skipped.
    fn try_submit(&self, job: ReadaheadJob) -> bool {
        let submitted = self.jobs.try_send(job).is_ok();
        if !submitted {
            lag_stats::record_readahead_skipped();
        }
        submitted
    }
}

struct StartGate {
    item_id: String,
    primed: Arc<AtomicBool>,
    deadline: Instant,
}

/// One feed's view of the readahead thread. Called from the feed tick.
pub(super) struct FeedReadahead {
    readahead: PcmReadahead,
    lead_bytes: usize,
    next_refresh: Instant,
    gate: Option<StartGate>,
}

impl FeedReadahead {
    pub(super) fn new(readahead: PcmReadahead, sample_rate: u32, channels: u16) -> Self {
        let bytes_per_second = sample_rate as usize * usize::from(channels.max(1)) * 2;
        Self {
            readahead,
            lead_bytes: bytes_per_second * READAHEAD_LEAD_MS / 1_000,
            next_refresh: Instant::now(),
            gate: None,
        }
    }

    /// Requests the pending item's first seconds once, and reports whether it may
    /// start: true once they are paged in, or after `START_WAIT` either way.
    pub(super) fn prime_pending(
        &mut self,
        pending: Option<&AudioItem>,
        groups: &HashMap<String, SegmentGroupPlayback>,
        now: Instant,
    ) -> bool {
        let Some(item) = pending else {
            self.gate = None;
            return true;
        };
        if self
            .gate
            .as_ref()
            .is_none_or(|gate| gate.item_id != item.id)
        {
            let primed = Arc::new(AtomicBool::new(false));
            let ranges = file_backed_ranges(upcoming_ranges(item, 0, groups, self.lead_bytes));
            let queued = !ranges.is_empty()
                && self.readahead.try_submit(ReadaheadJob {
                    ranges,
                    done: Some(Arc::clone(&primed)),
                });
            if !queued {
                primed.store(true, Ordering::Release);
            }
            self.gate = Some(StartGate {
                item_id: item.id.clone(),
                primed,
                deadline: now + START_WAIT,
            });
        }
        self.gate
            .as_ref()
            .is_none_or(|gate| gate.primed.load(Ordering::Acquire) || now >= gate.deadline)
    }

    /// Keeps the next seconds of the current and pending items paged in.
    pub(super) fn refresh(
        &mut self,
        current: Option<&AudioItem>,
        position: usize,
        pending: Option<&AudioItem>,
        groups: &HashMap<String, SegmentGroupPlayback>,
        now: Instant,
    ) {
        if now < self.next_refresh {
            return;
        }
        self.next_refresh = now + READAHEAD_REFRESH;
        let mut ranges = Vec::new();
        if let Some(item) = current {
            ranges.extend(upcoming_ranges(item, position, groups, self.lead_bytes));
        }
        if let Some(item) = pending {
            ranges.extend(upcoming_ranges(item, 0, groups, self.lead_bytes));
        }
        let ranges = file_backed_ranges(ranges);
        if !ranges.is_empty() {
            self.readahead
                .try_submit(ReadaheadJob { ranges, done: None });
        }
    }
}

fn file_backed_ranges(ranges: Vec<(SharedPcm, Range<usize>)>) -> Vec<(SharedPcm, Range<usize>)> {
    ranges
        .into_iter()
        .filter(|(pcm, range)| matches!(pcm, SharedPcm::Mapped(_)) && !range.is_empty())
        .collect()
}

// The next `lead_bytes` of PCM an item will play. Segment groups track their own
// position and span segments, stopping at the first one not yet available.
fn upcoming_ranges(
    item: &AudioItem,
    position: usize,
    groups: &HashMap<String, SegmentGroupPlayback>,
    lead_bytes: usize,
) -> Vec<(SharedPcm, Range<usize>)> {
    if item.segment_group.is_none() {
        let start = position.min(item.pcm.len());
        let end = start.saturating_add(lead_bytes).min(item.pcm.len());
        return vec![(item.pcm.clone(), start..end)];
    }
    let Some(group) = groups.get(&item.id) else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let mut remaining = lead_bytes;
    let mut offset = group.segment_offset;
    for segment in group.segments.iter().skip(group.segment_index) {
        let Some(segment) = segment else {
            break;
        };
        let start = offset.min(segment.pcm.len());
        let end = start.saturating_add(remaining).min(segment.pcm.len());
        ranges.push((segment.pcm.clone(), start..end));
        remaining -= end - start;
        if remaining == 0 {
            break;
        }
        offset = 0;
    }
    ranges
}

// Reading one byte per page faults the whole range in. Only the load matters, and
// `black_box` keeps it from being optimized away.
fn touch_pages(pcm: &[u8]) {
    for byte in pcm.iter().step_by(PAGE_STRIDE) {
        black_box(*byte);
    }
    if let Some(last) = pcm.last() {
        black_box(*last);
    }
}

#[cfg(test)]
mod tests {
    use super::super::map_raw_pcm_file;
    use super::super::tests::{
        pcm16, test_audio_item_with_pcm, test_group_item, test_group_revision, test_group_segment,
    };
    use super::super::{SegmentGroupStatus, SharedPcm};
    use super::*;

    // The directory comes first so callers drop the mapping before deleting it.
    fn mapped_item(id: &str, bytes: usize) -> (tempfile::TempDir, AudioItem) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("audio.raw");
        std::fs::write(&path, vec![1u8; bytes]).expect("write audio");
        let mut item = test_audio_item_with_pcm(id, id, Vec::new());
        item.pcm = map_raw_pcm_file(&path, 1).expect("map audio");
        (dir, item)
    }

    fn range_lengths(ranges: &[(SharedPcm, Range<usize>)]) -> Vec<Range<usize>> {
        ranges.iter().map(|(_, range)| range.clone()).collect()
    }

    #[test]
    fn item_range_starts_at_position_and_stops_at_the_end_of_the_audio() {
        let item = test_audio_item_with_pcm("item", "item", vec![0; 100]);
        let groups = HashMap::new();

        let middle = upcoming_ranges(&item, 10, &groups, 30);
        let near_end = upcoming_ranges(&item, 90, &groups, 30);

        assert_eq!(range_lengths(&middle), vec![10..40]);
        assert_eq!(range_lengths(&near_end), vec![90..100]);
    }

    #[test]
    fn group_ranges_span_segments_and_stop_at_a_missing_segment() {
        let revision = test_group_revision(
            "group-1",
            1,
            3,
            SegmentGroupStatus::Open,
            vec![
                test_group_segment(0, &[1, 2, 3, 4], 0),
                test_group_segment(1, &[5, 6], 0),
            ],
        );
        let item = test_group_item(Arc::clone(&SegmentGroupPlayback::new(&revision).group));
        let mut group = SegmentGroupPlayback::new(&revision);
        group.apply_revision(revision).expect("apply revision");
        group.segment_offset = pcm16(&[1]).len();
        let groups = HashMap::from([(item.id.clone(), group)]);

        // Asks for more than remains, so the missing third segment ends the walk.
        let all = upcoming_ranges(&item, 0, &groups, 1_000);
        // Asks for less than remains, so the lead ends the walk mid-segment.
        let partial = upcoming_ranges(&item, 0, &groups, 8);

        assert_eq!(range_lengths(&all), vec![2..8, 0..4]);
        assert_eq!(range_lengths(&partial), vec![2..8, 0..2]);
    }

    #[tokio::test]
    async fn heap_backed_pending_item_starts_without_waiting() {
        let readahead = PcmReadahead::spawn().expect("spawn readahead");
        let mut feed = FeedReadahead::new(readahead, 48_000, 1);
        let item = test_audio_item_with_pcm("heap", "heap", vec![0; 4_096]);

        assert!(feed.prime_pending(Some(&item), &HashMap::new(), Instant::now()));
    }

    #[tokio::test]
    async fn mapped_pending_item_starts_once_the_thread_has_paged_it_in() {
        let readahead = PcmReadahead::spawn().expect("spawn readahead");
        let mut feed = FeedReadahead::new(readahead, 48_000, 1);
        let (_dir, item) = mapped_item("mapped", 8_192);
        let groups = HashMap::new();

        let started = Instant::now();
        while !feed.prime_pending(Some(&item), &groups, started) {
            assert!(started.elapsed() < Duration::from_secs(5));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn pending_item_starts_at_the_deadline_when_the_thread_is_behind() {
        // A sender with no consumer: the job is accepted but never completes.
        let (jobs, _requests) = sync_channel::<ReadaheadJob>(1);
        let mut feed = FeedReadahead::new(PcmReadahead { jobs }, 48_000, 1);
        let (_dir, item) = mapped_item("mapped", 8_192);
        let groups = HashMap::new();
        let now = Instant::now();

        assert!(!feed.prime_pending(Some(&item), &groups, now));
        assert!(!feed.prime_pending(Some(&item), &groups, now + START_WAIT / 2));
        assert!(feed.prime_pending(Some(&item), &groups, now + START_WAIT));
    }

    #[tokio::test]
    async fn pending_item_starts_at_once_when_the_queue_is_full() {
        let (jobs, _requests) = sync_channel::<ReadaheadJob>(0);
        let mut feed = FeedReadahead::new(PcmReadahead { jobs }, 48_000, 1);
        let (_dir, item) = mapped_item("mapped", 8_192);

        assert!(feed.prime_pending(Some(&item), &HashMap::new(), Instant::now()));
    }
}
