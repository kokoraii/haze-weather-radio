use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, Local, Timelike, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::ServiceHostConfig;

const DEFAULT_SETTINGS_FILE: &str = "runtime/state/daemonSettings.json";
const ALERT_QUEUE_DIR: &str = "runtime/queues/alerts";
const AUTOMATION_DISPATCH_STATE_FILE: &str = "state/automation-dispatches.json";
const AUTOMATION_DISPATCH_CATCHUP_SECONDS: i64 = 24 * 60 * 60;
const AUTOMATION_DISPATCH_RETENTION_DAYS: i64 = 14;
const AUTOMATION_DISPATCH_RETRY_SECONDS: u64 = 30;
const AUTOMATION_ACCEPTED_RETRY_SECONDS: u64 = 5 * 60;
const AUTOMATION_ACK_BUFFER: usize = 256;
const DEFAULT_AUTOMATION_SECONDS: [u32; 1] = [0];

#[derive(Debug, Default, Deserialize)]
struct RootConfig {
    feeds_file: Option<String>,
    services: Option<ServicesConfig>,
    playout: Option<PlayoutConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct ServicesConfig {
    daemon: Option<DaemonServicesConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct DaemonServicesConfig {
    enabled: Option<bool>,
    scheduler: Option<DaemonSchedulerConfig>,
    playlist: Option<DaemonPlaylistConfig>,
    alert_queue: Option<DaemonAlertQueueConfig>,
    runtime_cleanup: Option<DaemonRuntimeCleanupConfig>,
    split_dns: Option<DaemonSplitDnsConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct DaemonSchedulerConfig {
    enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct DaemonPlaylistConfig {
    enabled: Option<bool>,
    interval_ms: Option<FlexibleU64>,
}

#[derive(Debug, Default, Deserialize)]
struct DaemonAlertQueueConfig {
    enabled: Option<bool>,
    interval_ms: Option<FlexibleU64>,
}

#[derive(Debug, Default, Deserialize)]
struct DaemonRuntimeCleanupConfig {
    enabled: Option<bool>,
    interval_minutes: Option<FlexibleU64>,
    rules: Option<Vec<RuntimeCleanupRuleConfig>>,
}

#[derive(Debug, Default, Deserialize)]
struct DaemonSplitDnsConfig {
    enabled: Option<bool>,
    listen: Option<String>,
    ttl_seconds: Option<FlexibleU64>,
    records: Option<Vec<SplitDnsRecordConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
struct SplitDnsRecordConfig {
    name: String,
    address: String,
    enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct RuntimeCleanupRuleConfig {
    path: String,
    max_age_minutes: Option<FlexibleU64>,
    enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct PlayoutConfig {
    station_id_schedule: Option<MinuteScheduleConfig>,
    date_time_schedule: Option<MinuteScheduleConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct MinuteScheduleConfig {
    enabled: Option<bool>,
    minutes: Option<MinuteList>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MinuteList {
    One(u8),
    Many(Vec<u8>),
    Text(String),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum FlexibleU64 {
    Number(u64),
    Text(String),
}

impl FlexibleU64 {
    fn value(&self) -> Option<u64> {
        match self {
            Self::Number(value) => Some(*value),
            Self::Text(value) => value.trim().parse().ok(),
        }
    }
}

pub(crate) struct DaemonServices {
    stop: Arc<AtomicBool>,
    threads: Vec<thread::JoinHandle<()>>,
    automation_ack: Option<SyncSender<AutomationDispatchAck>>,
}

impl DaemonServices {
    pub(crate) fn start(
        host: &ServiceHostConfig,
        publisher: Sender<Value>,
        media_publisher: Sender<Value>,
    ) -> Result<Self> {
        let config: RootConfig =
            decode_config_with_overlay(&host.config_path, &host.app_dir, &host.runtime_dir)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut automation_ack = None;
        let Some(daemon_cfg) = config
            .services
            .as_ref()
            .and_then(|services| services.daemon.as_ref())
        else {
            return Ok(Self {
                stop,
                threads: Vec::new(),
                automation_ack,
            });
        };
        if !daemon_cfg.enabled.unwrap_or(false) {
            return Ok(Self {
                stop,
                threads: Vec::new(),
                automation_ack,
            });
        }

        let mut threads = Vec::new();
        if daemon_cfg
            .scheduler
            .as_ref()
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(false)
        {
            let schedules = SchedulePlan::from_config(
                config.playout.as_ref(),
                &host.config_path,
                config.feeds_file.as_deref(),
            );
            let automation_count = schedules.automations.len();
            let automation_target_count = schedules
                .automations
                .iter()
                .map(|automation| automation.targets.len())
                .sum::<usize>();
            let tx = publisher.clone();
            let stop_flag = Arc::clone(&stop);
            let runtime_dir = host.runtime_dir.clone();
            let (automation_ack_tx, automation_ack_rx) = sync_channel(AUTOMATION_ACK_BUFFER);
            automation_ack = Some(automation_ack_tx);
            threads.push(thread::spawn(move || {
                scheduler_loop(schedules, tx, automation_ack_rx, stop_flag, runtime_dir);
            }));
            info!(
                automation_count,
                automation_target_count,
                automation_dispatch_catchup_seconds = AUTOMATION_DISPATCH_CATCHUP_SECONDS,
                "daemon scheduler service enabled"
            );
        }

        if daemon_cfg
            .alert_queue
            .as_ref()
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(true)
        {
            let interval_ms = daemon_cfg
                .alert_queue
                .as_ref()
                .and_then(|cfg| cfg.interval_ms.as_ref())
                .and_then(|value| value.value())
                .unwrap_or(500)
                .clamp(100, 10_000);
            let runtime_dir = host.runtime_dir.clone();
            let tx = publisher.clone();
            let media_tx = media_publisher.clone();
            let stop_flag = Arc::clone(&stop);
            threads.push(thread::spawn(move || {
                alert_queue_loop(runtime_dir, interval_ms, tx, media_tx, stop_flag);
            }));
            info!(interval_ms, "daemon alert queue service enabled");
        }

        if daemon_cfg
            .playlist
            .as_ref()
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(false)
        {
            let interval_ms = daemon_cfg
                .playlist
                .as_ref()
                .and_then(|cfg| cfg.interval_ms.as_ref())
                .and_then(|value| value.value())
                .unwrap_or(750)
                .clamp(100, 10_000);
            let tx = publisher;
            let stop_flag = Arc::clone(&stop);
            threads.push(thread::spawn(move || {
                playlist_loop(interval_ms, tx, stop_flag);
            }));
            info!(interval_ms, "daemon playlist service enabled");
        }

        if daemon_cfg
            .runtime_cleanup
            .as_ref()
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(false)
        {
            let cleanup_cfg = daemon_cfg.runtime_cleanup.as_ref();
            let interval_minutes = cleanup_cfg
                .and_then(|cfg| cfg.interval_minutes.as_ref())
                .and_then(|value| value.value())
                .unwrap_or(30)
                .clamp(1, 24 * 60);
            let rules = runtime_cleanup_rules(cleanup_cfg)
                .into_iter()
                .map(|mut rule| {
                    rule.relative_path = crate::runtime_dir::configured_runtime_relative_path(
                        &host.app_dir,
                        &host.runtime_dir,
                        &rule.relative_path,
                    );
                    rule
                })
                .collect::<Vec<_>>();
            let rule_count = rules.len();
            let runtime_dir = host.runtime_dir.clone();
            let stop_flag = Arc::clone(&stop);
            threads.push(thread::spawn(move || {
                runtime_cleanup_loop(runtime_dir, interval_minutes, rules, stop_flag);
            }));
            info!(
                interval_minutes,
                rule_count, "daemon runtime cleanup service enabled"
            );
        }

        if daemon_cfg
            .split_dns
            .as_ref()
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(false)
        {
            let dns_cfg = daemon_cfg.split_dns.as_ref();
            let listen = dns_cfg
                .and_then(|cfg| cfg.listen.as_deref())
                .filter(|raw| !raw.trim().is_empty())
                .unwrap_or("0.0.0.0:53")
                .trim()
                .to_string();
            let ttl_seconds = dns_cfg
                .and_then(|cfg| cfg.ttl_seconds.as_ref())
                .and_then(FlexibleU64::value)
                .unwrap_or(60)
                .clamp(1, 86_400) as u32;
            let records = split_dns_records(dns_cfg);
            let record_count = records.len();
            if record_count == 0 {
                warn!("daemon split dns enabled without usable A records");
            } else {
                let stop_flag = Arc::clone(&stop);
                let listen_for_thread = listen.clone();
                threads.push(thread::spawn(move || {
                    split_dns_loop(listen_for_thread, records, ttl_seconds, stop_flag);
                }));
                info!(
                    listen,
                    record_count, ttl_seconds, "daemon split dns service enabled"
                );
            }
        }

        Ok(Self {
            stop,
            threads,
            automation_ack,
        })
    }

    pub(crate) fn handle_event(&self, event: &Value) {
        let Some(ack) = automation_dispatch_ack(event) else {
            return;
        };
        let Some(sender) = &self.automation_ack else {
            return;
        };
        if let Err(error) = sender.try_send(ack) {
            warn!(%error, "automation acknowledgement queue is full or unavailable");
        }
    }

    pub(crate) fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        while let Some(thread) = self.threads.pop() {
            if thread.join().is_err() {
                warn!("managed service thread exited unexpectedly during shutdown");
            }
        }
    }
}

impl Drop for DaemonServices {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[derive(Debug, Clone)]
struct SchedulePlan {
    station_id_minutes: Vec<u8>,
    date_time_minutes: Vec<u8>,
    automations: Vec<AutomationPlan>,
}

impl SchedulePlan {
    fn from_config(
        playout: Option<&PlayoutConfig>,
        config_path: &Path,
        feeds_file: Option<&str>,
    ) -> Self {
        let station_id = playout
            .and_then(|cfg| cfg.station_id_schedule.as_ref())
            .map(|cfg| cfg.minutes_or_default(&[0, 15, 30, 45]))
            .unwrap_or_else(|| vec![0, 15, 30, 45]);
        let date_time = playout
            .and_then(|cfg| cfg.date_time_schedule.as_ref())
            .map(|cfg| cfg.minutes_or_default(&[5, 15, 25, 35, 45, 55]))
            .unwrap_or_else(|| vec![5, 15, 25, 35, 45, 55]);
        let station_id_minutes = if playout
            .and_then(|cfg| cfg.station_id_schedule.as_ref())
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(true)
        {
            station_id
        } else {
            Vec::new()
        };
        let date_time_minutes = if playout
            .and_then(|cfg| cfg.date_time_schedule.as_ref())
            .and_then(|cfg| cfg.enabled)
            .unwrap_or(true)
        {
            date_time
        } else {
            Vec::new()
        };
        let automation_feeds =
            load_automation_feeds(config_path, feeds_file).unwrap_or_else(|err| {
                warn!("automation feed timezones unavailable: {err}");
                Vec::new()
            });
        Self {
            station_id_minutes,
            date_time_minutes,
            automations: load_automation_plan(config_path, &automation_feeds).unwrap_or_else(
                |err| {
                    warn!("automation schedule unavailable: {err}");
                    Vec::new()
                },
            ),
        }
    }
}

impl MinuteScheduleConfig {
    fn minutes_or_default(&self, default: &[u8]) -> Vec<u8> {
        let mut minutes = match &self.minutes {
            Some(MinuteList::One(value)) => vec![*value],
            Some(MinuteList::Many(values)) => values.clone(),
            Some(MinuteList::Text(raw)) => raw
                .split(',')
                .filter_map(|part| part.trim().parse::<u8>().ok())
                .collect(),
            None => default.to_vec(),
        };
        minutes.retain(|minute| *minute <= 59);
        minutes.sort_unstable();
        minutes.dedup();
        if minutes.is_empty() {
            default.to_vec()
        } else {
            minutes
        }
    }
}

#[derive(Debug, Clone)]
struct AutomationPlan {
    id: String,
    name: String,
    description: String,
    schedule: AutomationSchedule,
    same: AutomationSame,
    content: AutomationContent,
    targets: Vec<AutomationFeedTarget>,
}

#[derive(Debug, Clone, Default)]
struct AutomationSchedule {
    months: Vec<u32>,
    weeks: Vec<AutomationWeek>,
    days: Vec<u32>,
    weekdays: Vec<u32>,
    hours: Vec<u32>,
    minutes: Vec<u32>,
    seconds: Vec<u32>,
}

#[derive(Debug, Clone)]
struct AutomationWeek {
    week: u32,
    event_override: String,
}

#[derive(Debug, Clone)]
struct AutomationSame {
    enabled: bool,
    event: String,
    originator: String,
    locations: Vec<String>,
    duration: String,
    sender_id: String,
    tone: String,
}

#[derive(Debug, Clone)]
struct AutomationContent {
    text: String,
    reader_id: String,
}

#[derive(Debug, Clone)]
struct AutomationFeedTarget {
    feed_id: String,
    timezone: ScheduleZone,
}

#[derive(Debug, Clone)]
struct AutomationFeed {
    id: String,
    timezone: ScheduleZone,
}

#[derive(Debug, Clone)]
enum ScheduleZone {
    Local,
    Named(Tz),
}

impl ScheduleZone {
    fn from_raw(raw: &str) -> Option<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("local") {
            return Some(Self::Local);
        }
        trimmed.parse::<Tz>().ok().map(Self::Named)
    }

    fn name(&self) -> String {
        match self {
            Self::Local => "Local".to_string(),
            Self::Named(zone) => zone.name().to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SchedulerFeedsXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<SchedulerFeedXml>,
}

#[derive(Debug, Default, Deserialize)]
struct SchedulerFeedXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(rename = "@enabled", default)]
    enabled: String,
    #[serde(rename = "@timezone", default)]
    timezone: String,
    #[serde(default)]
    playout: SchedulerFeedPlayoutXml,
}

#[derive(Debug, Default, Deserialize)]
struct SchedulerFeedPlayoutXml {
    #[serde(rename = "@routine", default)]
    routine: String,
    #[serde(rename = "@same", default)]
    same: String,
}

#[derive(Debug, Deserialize)]
struct AutomationsXml {
    #[serde(rename = "automation", default)]
    automations: Vec<AutomationXml>,
    #[serde(rename = "template", default)]
    templates: Vec<AutomationXml>,
}

#[derive(Debug, Deserialize)]
struct AutomationXml {
    #[serde(rename = "@id", default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    enabled: String,
    #[serde(default)]
    automated: AutomationAutomatedXml,
    #[serde(default)]
    schedule: AutomationScheduleXml,
    #[serde(default)]
    same: AutomationSameXml,
    #[serde(default)]
    content: AutomationContentXml,
    #[serde(default)]
    target: AutomationTargetXml,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationAutomatedXml {
    #[serde(default)]
    enabled: String,
    #[serde(default)]
    timing: AutomationScheduleXml,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationScheduleXml {
    #[serde(default)]
    months: String,
    #[serde(default)]
    days: String,
    #[serde(default)]
    weekdays: String,
    #[serde(default)]
    hours: String,
    #[serde(default)]
    minutes: String,
    #[serde(default)]
    seconds: String,
    #[serde(default)]
    weeks: AutomationWeeksXml,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationWeeksXml {
    #[serde(rename = "week", default)]
    weeks: Vec<AutomationWeekXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationWeekXml {
    #[serde(rename = "@event_override", default)]
    event_override: String,
    #[serde(rename = "$text", default)]
    value: String,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationSameXml {
    #[serde(default)]
    enabled: String,
    #[serde(default)]
    event: String,
    #[serde(default)]
    originator: String,
    #[serde(default)]
    locations: AutomationLocationsXml,
    #[serde(default)]
    duration: AutomationDurationXml,
    #[serde(default)]
    sender_id: String,
    #[serde(default)]
    tone: String,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationLocationsXml {
    #[serde(rename = "location", default)]
    locations: Vec<AutomationLocationXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationLocationXml {
    #[serde(rename = "@id", default)]
    id: String,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationDurationXml {
    #[serde(rename = "@hr", default)]
    hours: String,
    #[serde(rename = "@min", default)]
    minutes: String,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationContentXml {
    #[serde(rename = "@reader_id", default)]
    reader_id: String,
    #[serde(rename = "lang", default)]
    langs: Vec<AutomationLangXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationLangXml {
    #[serde(rename = "@code", default)]
    code: String,
    #[serde(default)]
    text: String,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationTargetXml {
    #[serde(rename = "feed", default)]
    feeds: Vec<AutomationFeedXml>,
}

#[derive(Debug, Default, Deserialize)]
struct AutomationFeedXml {
    #[serde(rename = "@id", default)]
    id: String,
}

#[derive(Debug, Clone)]
struct DueAutomation {
    event: String,
    scheduled_for: DateTime<Utc>,
}

impl AutomationPlan {
    #[cfg(test)]
    fn matching_event_for_target(
        &self,
        now: DateTime<Utc>,
        target: &AutomationFeedTarget,
    ) -> Option<String> {
        match &target.timezone {
            ScheduleZone::Local => {
                let local = now.with_timezone(&Local);
                self.matching_event_at(&local)
            }
            ScheduleZone::Named(zone) => {
                let local = now.with_timezone(zone);
                self.matching_event_at(&local)
            }
        }
    }

    fn due_for_target(
        &self,
        now: DateTime<Utc>,
        target: &AutomationFeedTarget,
    ) -> Option<DueAutomation> {
        match &target.timezone {
            ScheduleZone::Local => {
                let local = now.with_timezone(&Local);
                self.due_at(&local)
            }
            ScheduleZone::Named(zone) => {
                let local = now.with_timezone(zone);
                self.due_at(&local)
            }
        }
    }

    fn due_at<TzImpl: chrono::TimeZone>(
        &self,
        now: &chrono::DateTime<TzImpl>,
    ) -> Option<DueAutomation> {
        let catchup = ChronoDuration::seconds(AUTOMATION_DISPATCH_CATCHUP_SECONDS);
        let minutes_to_check = (AUTOMATION_DISPATCH_CATCHUP_SECONDS + 59) / 60;
        for minute_offset in 0..=minutes_to_check {
            let minute = now.clone() - ChronoDuration::minutes(minute_offset);
            for &second in self.schedule_seconds() {
                let Some(candidate) = minute
                    .with_second(second)
                    .and_then(|candidate| candidate.with_nanosecond(0))
                else {
                    continue;
                };
                let age = now.clone().signed_duration_since(candidate.clone());
                if age < ChronoDuration::zero() || age > catchup {
                    continue;
                }
                if let Some(event) = self.matching_event_at(&candidate) {
                    return Some(DueAutomation {
                        event,
                        scheduled_for: candidate.with_timezone(&Utc),
                    });
                }
            }
        }
        None
    }

    fn matching_event_at<TzImpl: chrono::TimeZone>(
        &self,
        now: &chrono::DateTime<TzImpl>,
    ) -> Option<String> {
        if !matches_list(&self.schedule.months, now.month()) {
            return None;
        }
        if !matches_list(&self.schedule.days, now.day()) {
            return None;
        }
        if !matches_list(&self.schedule.weekdays, now.weekday().number_from_monday()) {
            return None;
        }
        if !matches_list(&self.schedule.hours, now.hour()) {
            return None;
        }
        if !matches_list(&self.schedule.minutes, now.minute()) {
            return None;
        }
        if !self.schedule_seconds().contains(&now.second()) {
            return None;
        }
        let week = week_of_month(now.day());
        if self.schedule.weeks.is_empty() {
            return Some(self.same.event.clone());
        }
        for item in &self.schedule.weeks {
            let configured_week = if item.week == 0 { 1 } else { item.week };
            if configured_week == week {
                if item.event_override.trim().is_empty() {
                    return Some(self.same.event.clone());
                }
                return Some(item.event_override.trim().to_uppercase());
            }
        }
        None
    }

    fn schedule_seconds(&self) -> &[u32] {
        if self.schedule.seconds.is_empty() {
            &DEFAULT_AUTOMATION_SECONDS
        } else {
            &self.schedule.seconds
        }
    }

    fn dispatch_key(&self, target: &AutomationFeedTarget, scheduled_for: DateTime<Utc>) -> String {
        format!(
            "{}:{}:{}",
            self.id,
            target.feed_id,
            scheduled_for.timestamp_millis()
        )
    }

    fn event_payload(
        &self,
        event: String,
        target: &AutomationFeedTarget,
        scheduled_for: DateTime<Utc>,
    ) -> Value {
        let feed_id = target.feed_id.clone();
        let scheduled_for_ms = scheduled_for.timestamp_millis();
        let dispatch_key = self.dispatch_key(target, scheduled_for);
        json!({
            "type": "cap.alert.broadcast.requested",
            "source": "automation",
            "data": {
                "alert_id": format!("automation-{}-{}-{}", self.id, feed_id, scheduled_for_ms),
                "automation_id": self.id,
                "title": self.name,
                "dispatch_key": dispatch_key,
                "description": self.description,
                "feed_id": feed_id.clone(),
                "feed_ids": [feed_id],
                "timezone": target.timezone.name(),
                "scheduled_for": scheduled_for.to_rfc3339(),
                "include_same": self.same.enabled,
                "same_originator": self.same.originator,
                "same_event": event,
                "event": event,
                "same_locations": self.same.locations,
                "same_duration": self.same.duration,
                "same_callsign": self.same.sender_id,
                "same_tone": self.same.tone,
                "alert_text": self.content.text,
                "message_type": "Alert",
                "reader_id": self.content.reader_id,
                "alert_sent_at": Utc::now().to_rfc3339(),
            }
        })
    }
}

fn load_automation_plan(
    config_path: &Path,
    automation_feeds: &[AutomationFeed],
) -> Result<Vec<AutomationPlan>> {
    let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let path = resolve_path(base_dir, Path::new("managed/configs/automations.xml"));
    let fallback_path = resolve_path(base_dir, Path::new("managed/configs/alertTemplates.xml"));
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::read_to_string(&fallback_path).unwrap_or_default()
        }
        Err(err) => return Err(err).with_context(|| format!("failed to read {}", path.display())),
    };
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: AutomationsXml =
        quick_xml::de::from_str(&raw).context("failed to parse automations XML")?;
    let mut plans = Vec::new();
    let mut items = parsed.automations;
    items.extend(parsed.templates);
    for (index, item) in items.into_iter().enumerate() {
        let enabled_raw = fallback_string(item.enabled.clone(), item.automated.enabled.clone());
        if !xml_bool(&enabled_raw, true) {
            continue;
        }
        let event = item.same.event.trim().to_uppercase();
        let id = fallback_string(item.id, format!("automation{}", index + 1));
        let schedule_xml = if has_schedule_xml(&item.schedule) {
            item.schedule
        } else {
            item.automated.timing
        };
        plans.push(AutomationPlan {
            id: id.clone(),
            name: fallback_string(item.name, event.clone()),
            description: item.description.trim().to_string(),
            schedule: AutomationSchedule {
                months: parse_number_list(&schedule_xml.months, 1, 12),
                days: parse_number_list(&schedule_xml.days, 1, 31),
                weekdays: parse_weekday_list(&schedule_xml.weekdays),
                hours: parse_number_list(&schedule_xml.hours, 0, 23),
                minutes: parse_number_list(&schedule_xml.minutes, 0, 59),
                seconds: parse_number_list(&schedule_xml.seconds, 0, 59),
                weeks: schedule_xml
                    .weeks
                    .weeks
                    .into_iter()
                    .filter_map(|week| {
                        let parsed = week.value.trim().parse::<u32>().ok()?;
                        if parsed <= 5 {
                            Some(AutomationWeek {
                                week: parsed,
                                event_override: week.event_override,
                            })
                        } else {
                            None
                        }
                    })
                    .collect(),
            },
            same: AutomationSame {
                enabled: xml_bool(&item.same.enabled, true),
                event,
                originator: fallback_string(item.same.originator, "WXR".to_string()).to_uppercase(),
                locations: item
                    .same
                    .locations
                    .locations
                    .into_iter()
                    .map(|location| location.id.trim().to_string())
                    .filter(|id| !id.is_empty())
                    .collect(),
                duration: automation_duration(item.same.duration),
                sender_id: item.same.sender_id.trim().to_string(),
                tone: fallback_string(item.same.tone, "WXR".to_string()).to_uppercase(),
            },
            content: AutomationContent {
                text: automation_text(&item.content),
                reader_id: item.content.reader_id.trim().to_string(),
            },
            targets: automation_targets(item.target, automation_feeds),
        });
    }
    Ok(plans)
}

fn automation_duration(duration: AutomationDurationXml) -> String {
    let hours = duration.hours.trim().parse::<u32>().unwrap_or(0).min(99);
    let minutes = duration.minutes.trim().parse::<u32>().unwrap_or(15).min(59);
    format!("{hours:02}{minutes:02}")
}

fn has_schedule_xml(schedule: &AutomationScheduleXml) -> bool {
    !schedule.months.trim().is_empty()
        || !schedule.days.trim().is_empty()
        || !schedule.weekdays.trim().is_empty()
        || !schedule.hours.trim().is_empty()
        || !schedule.minutes.trim().is_empty()
        || !schedule.seconds.trim().is_empty()
        || !schedule.weeks.weeks.is_empty()
}

fn automation_text(content: &AutomationContentXml) -> String {
    for lang in &content.langs {
        if lang.code.trim().eq_ignore_ascii_case("en")
            || lang.code.trim().eq_ignore_ascii_case("en-CA")
        {
            return lang.text.trim().to_string();
        }
    }
    content
        .langs
        .first()
        .map(|lang| lang.text.trim().to_string())
        .unwrap_or_default()
}

fn load_automation_feeds(
    config_path: &Path,
    feeds_file: Option<&str>,
) -> Result<Vec<AutomationFeed>> {
    let base_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
    let feeds_path = resolve_path(
        base_dir,
        Path::new(
            feeds_file
                .filter(|raw| !raw.trim().is_empty())
                .unwrap_or("managed/configs/feeds.xml"),
        ),
    );
    let raw = fs::read_to_string(&feeds_path)
        .with_context(|| format!("failed to read feeds XML {}", feeds_path.display()))?;
    let raw = expand_env_vars(&raw);
    let parsed: SchedulerFeedsXml = quick_xml::de::from_str(&raw)
        .with_context(|| format!("failed to parse feeds XML {}", feeds_path.display()))?;
    let mut feeds = Vec::new();
    for feed in parsed.feeds {
        let id = feed.id.trim().to_string();
        if id.is_empty() || !xml_bool(&feed.enabled, true) {
            continue;
        }
        let routine_enabled = xml_bool(&feed.playout.routine, true);
        let same_enabled = xml_bool(&feed.playout.same, true);
        if !routine_enabled && !same_enabled {
            continue;
        }
        let timezone = ScheduleZone::from_raw(&feed.timezone).unwrap_or_else(|| {
            warn!(
                feed_id = %id,
                timezone = %feed.timezone,
                "feed timezone is invalid, automation schedule will use daemon local time"
            );
            ScheduleZone::Local
        });
        feeds.push(AutomationFeed { id, timezone });
    }
    Ok(feeds)
}

fn automation_targets(
    target: AutomationTargetXml,
    automation_feeds: &[AutomationFeed],
) -> Vec<AutomationFeedTarget> {
    let mut feed_ids: Vec<String> = target
        .feeds
        .into_iter()
        .map(|feed| feed.id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect();
    feed_ids.sort();
    feed_ids.dedup();
    if feed_ids.is_empty() || feed_ids.iter().any(|id| id == "*") {
        if automation_feeds.is_empty() {
            return vec![AutomationFeedTarget {
                feed_id: "*".to_string(),
                timezone: ScheduleZone::Local,
            }];
        }
        return automation_feeds
            .iter()
            .map(|feed| AutomationFeedTarget {
                feed_id: feed.id.clone(),
                timezone: feed.timezone.clone(),
            })
            .collect();
    }
    feed_ids
        .into_iter()
        .map(|feed_id| {
            let timezone = automation_feeds
                .iter()
                .find(|feed| feed.id == feed_id)
                .map(|feed| feed.timezone.clone())
                .unwrap_or(ScheduleZone::Local);
            AutomationFeedTarget { feed_id, timezone }
        })
        .collect()
}

fn parse_number_list(raw: &str, min: u32, max: u32) -> Vec<u32> {
    raw.split(',')
        .filter_map(|part| part.trim().parse::<u32>().ok())
        .filter(|value| *value >= min && *value <= max)
        .collect()
}

fn parse_weekday_list(raw: &str) -> Vec<u32> {
    raw.split(',')
        .filter_map(|part| match part.trim().to_ascii_lowercase().as_str() {
            "mon" | "monday" | "1" => Some(1),
            "tue" | "tues" | "tuesday" | "2" => Some(2),
            "wed" | "wednesday" | "3" => Some(3),
            "thu" | "thur" | "thurs" | "thursday" | "4" => Some(4),
            "fri" | "friday" | "5" => Some(5),
            "sat" | "saturday" | "6" => Some(6),
            "sun" | "sunday" | "7" => Some(7),
            _ => None,
        })
        .collect()
}

fn matches_list(values: &[u32], actual: u32) -> bool {
    values.is_empty() || values.contains(&actual)
}

fn week_of_month(day: u32) -> u32 {
    ((day.saturating_sub(1)) / 7) + 1
}

fn fallback_string(value: String, fallback: String) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback
    } else {
        trimmed.to_string()
    }
}

fn xml_bool(raw: &str, fallback: bool) -> bool {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => fallback,
        "1" | "true" | "yes" | "on" | "enabled" => true,
        "0" | "false" | "no" | "off" | "disabled" => false,
        _ => fallback,
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct AutomationDispatchLedger {
    #[serde(default)]
    dispatched: HashMap<String, i64>,
}

impl AutomationDispatchLedger {
    fn load(path: &Path) -> Self {
        let raw = match fs::read(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Self::default(),
            Err(error) => {
                warn!(path = %path.display(), %error, "failed to read automation dispatch state");
                return Self::default();
            }
        };
        match serde_json::from_slice(&raw) {
            Ok(ledger) => ledger,
            Err(error) => {
                warn!(path = %path.display(), %error, "failed to decode automation dispatch state");
                Self::default()
            }
        }
    }

    fn has_dispatched(&self, key: &str) -> bool {
        self.dispatched.contains_key(key)
    }

    fn record(&mut self, key: String, scheduled_for: DateTime<Utc>) {
        self.dispatched.insert(key, scheduled_for.timestamp());
        let oldest_allowed = Utc::now().timestamp()
            - AUTOMATION_DISPATCH_RETENTION_DAYS.saturating_mul(24 * 60 * 60);
        self.dispatched
            .retain(|_, dispatched_at| *dispatched_at >= oldest_allowed);
    }

    fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "failed to create automation state directory {}",
                    parent.display()
                )
            })?;
        }
        let raw = serde_json::to_vec(self).context("failed to encode automation dispatch state")?;
        let temporary_path = path.with_extension("json.tmp");
        fs::write(&temporary_path, &raw).with_context(|| {
            format!(
                "failed to write automation dispatch state {}",
                temporary_path.display()
            )
        })?;
        if let Err(rename_error) = fs::rename(&temporary_path, path) {
            // Windows does not replace an existing destination with rename. A direct write is
            // still safe here because this scheduler has one writer and the state only guards a
            // short catch-up window.
            fs::write(path, raw).with_context(|| {
                format!(
                    "failed to replace automation dispatch state {} after rename error {}",
                    path.display(),
                    rename_error
                )
            })?;
            let _ = fs::remove_file(&temporary_path);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutomationDispatchStatus {
    Accepted,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
struct AutomationDispatchAck {
    dispatch_key: String,
    scheduled_for: Option<DateTime<Utc>>,
    status: AutomationDispatchStatus,
}

#[derive(Debug)]
struct PendingAutomationDispatch {
    scheduled_for: DateTime<Utc>,
    last_attempt: Instant,
    accepted: bool,
}

fn automation_dispatch_ack(event: &Value) -> Option<AutomationDispatchAck> {
    let status = match event.get("type")?.as_str()? {
        "automation.dispatch.accepted" => AutomationDispatchStatus::Accepted,
        "automation.dispatch.completed" => AutomationDispatchStatus::Completed,
        "automation.dispatch.failed" => AutomationDispatchStatus::Failed,
        _ => return None,
    };
    let data = event.get("data")?;
    let dispatch_key = data.get("dispatch_key")?.as_str()?.trim().to_string();
    if dispatch_key.is_empty() {
        return None;
    }
    let scheduled_for = data
        .get("scheduled_for")
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|value| value.with_timezone(&Utc));
    Some(AutomationDispatchAck {
        dispatch_key,
        scheduled_for,
        status,
    })
}

fn scheduler_loop(
    plan: SchedulePlan,
    publisher: Sender<Value>,
    automation_acks: Receiver<AutomationDispatchAck>,
    stop: Arc<AtomicBool>,
    runtime_dir: PathBuf,
) {
    let dispatch_state_path = runtime_dir.join(AUTOMATION_DISPATCH_STATE_FILE);
    let mut dispatch_ledger = AutomationDispatchLedger::load(&dispatch_state_path);
    let mut pending_dispatches = HashMap::<String, PendingAutomationDispatch>::new();
    let mut last_minute_key = String::new();
    let mut last_second_key = String::new();
    while !stop.load(Ordering::SeqCst) {
        while let Ok(ack) = automation_acks.try_recv() {
            match ack.status {
                AutomationDispatchStatus::Accepted => {
                    if let Some(pending) = pending_dispatches.get_mut(&ack.dispatch_key) {
                        pending.accepted = true;
                        pending.last_attempt = Instant::now();
                        info!(dispatch_key = %ack.dispatch_key, "automation dispatch accepted");
                    }
                }
                AutomationDispatchStatus::Completed => {
                    let scheduled_for = pending_dispatches
                        .remove(&ack.dispatch_key)
                        .map(|pending| pending.scheduled_for)
                        .or(ack.scheduled_for);
                    let Some(scheduled_for) = scheduled_for else {
                        warn!(dispatch_key = %ack.dispatch_key, "automation completion omitted scheduled time");
                        continue;
                    };
                    dispatch_ledger.record(ack.dispatch_key.clone(), scheduled_for);
                    if let Err(error) = dispatch_ledger.save(&dispatch_state_path) {
                        warn!(
                            path = %dispatch_state_path.display(),
                            %error,
                            "failed to persist automation dispatch state"
                        );
                    }
                    info!(dispatch_key = %ack.dispatch_key, "automation dispatch completed");
                }
                AutomationDispatchStatus::Failed => {
                    if let Some(pending) = pending_dispatches.get_mut(&ack.dispatch_key) {
                        pending.accepted = false;
                        pending.last_attempt = Instant::now();
                    }
                    warn!(
                        dispatch_key = %ack.dispatch_key,
                        "automation dispatch failed downstream and will be retried"
                    );
                }
            }
        }
        let now_utc = Utc::now();
        let now = now_utc.with_timezone(&Local);
        let second = now.second();
        let second_key = now_utc.format("%Y%m%dT%H%M%S").to_string();
        if second_key != last_second_key {
            for automation in &plan.automations {
                for target in &automation.targets {
                    let Some(due) = automation.due_for_target(now_utc, target) else {
                        continue;
                    };
                    let dispatch_key = automation.dispatch_key(target, due.scheduled_for);
                    if dispatch_ledger.has_dispatched(&dispatch_key) {
                        continue;
                    }
                    if let Some(pending) = pending_dispatches.get(&dispatch_key) {
                        let retry_after = Duration::from_secs(if pending.accepted {
                            AUTOMATION_ACCEPTED_RETRY_SECONDS
                        } else {
                            AUTOMATION_DISPATCH_RETRY_SECONDS
                        });
                        if pending.last_attempt.elapsed() < retry_after {
                            continue;
                        }
                    }
                    let event = due.event.clone();
                    let scheduled_for = due.scheduled_for;
                    if let Err(error) =
                        publisher.send(automation.event_payload(due.event, target, scheduled_for))
                    {
                        warn!(
                            automation_id = %automation.id,
                            feed_id = %target.feed_id,
                            timezone = %target.timezone.name(),
                            %event,
                            scheduled_for = %scheduled_for.to_rfc3339(),
                            %error,
                            "automation dispatch failed"
                        );
                        continue;
                    }
                    pending_dispatches.insert(
                        dispatch_key.clone(),
                        PendingAutomationDispatch {
                            scheduled_for,
                            last_attempt: Instant::now(),
                            accepted: false,
                        },
                    );
                    info!(
                        automation_id = %automation.id,
                        feed_id = %target.feed_id,
                        timezone = %target.timezone.name(),
                        %event,
                        scheduled_for = %scheduled_for.to_rfc3339(),
                        dispatch_key = %dispatch_key,
                        "automation dispatch requested"
                    );
                }
            }
            last_second_key = second_key;
        }
        if second == 0 {
            let minute = now.minute() as u8;
            let key = now.format("%Y%m%dT%H%M").to_string();
            if key != last_minute_key {
                if plan.station_id_minutes.contains(&minute) {
                    publish_scheduled_package(&publisher, "station_id");
                }
                if plan.date_time_minutes.contains(&minute) {
                    publish_scheduled_package(&publisher, "date_time");
                }
                last_minute_key = key;
            }
        }
        sleep_or_stopped(
            &stop,
            Duration::from_millis(if second == 59 { 100 } else { 250 }),
        );
    }
}

fn playlist_loop(interval_ms: u64, publisher: Sender<Value>, stop: Arc<AtomicBool>) {
    let mut first = true;
    while !stop.load(Ordering::SeqCst) {
        let message = json!({
            "type": "playlist_refill",
            "source": "daemon-playlist",
            "force": first,
            "timestamp_unix_ms": unix_now_ms(),
        });
        let _ = publisher.send(message);
        first = false;
        sleep_or_stopped(&stop, Duration::from_millis(interval_ms));
    }
}

#[derive(Debug, Clone)]
struct RuntimeCleanupRule {
    relative_path: PathBuf,
    max_age: Duration,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RuntimeCleanupStats {
    scanned_files: u64,
    removed_files: u64,
    removed_bytes: u64,
    error_count: u64,
}

fn runtime_cleanup_loop(
    runtime_dir: PathBuf,
    interval_minutes: u64,
    rules: Vec<RuntimeCleanupRule>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        let started_at = SystemTime::now();
        let stats = run_runtime_cleanup_once(&runtime_dir, &rules, started_at);
        if stats.removed_files > 0 || stats.error_count > 0 {
            info!(
                scanned_files = stats.scanned_files,
                removed_files = stats.removed_files,
                removed_mb = stats.removed_bytes / (1024 * 1024),
                error_count = stats.error_count,
                "runtime cleanup completed"
            );
        }
        sleep_or_stopped(
            &stop,
            Duration::from_secs(interval_minutes.saturating_mul(60)),
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SplitDnsRecord {
    name: String,
    address: Ipv4Addr,
}

fn split_dns_records(config: Option<&DaemonSplitDnsConfig>) -> Vec<SplitDnsRecord> {
    config
        .and_then(|cfg| cfg.records.as_ref())
        .map(|records| {
            records
                .iter()
                .filter(|record| record.enabled.unwrap_or(true))
                .filter_map(|record| {
                    let name = normalize_dns_name(&record.name)?;
                    let address = record.address.trim().parse::<Ipv4Addr>().ok()?;
                    Some(SplitDnsRecord { name, address })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn split_dns_loop(
    listen: String,
    records: Vec<SplitDnsRecord>,
    ttl_seconds: u32,
    stop: Arc<AtomicBool>,
) {
    let socket = match UdpSocket::bind(&listen) {
        Ok(socket) => socket,
        Err(err) => {
            warn!(%listen, error = %err, "daemon split dns failed to bind");
            return;
        }
    };
    if let Err(err) = socket.set_read_timeout(Some(Duration::from_millis(500))) {
        warn!(%listen, error = %err, "daemon split dns failed to set read timeout");
        return;
    }
    let tcp_listener = match TcpListener::bind(&listen) {
        Ok(listener) => match listener.set_nonblocking(true) {
            Ok(()) => Some(listener),
            Err(err) => {
                warn!(%listen, error = %err, "daemon split dns tcp failed to set nonblocking mode");
                None
            }
        },
        Err(err) => {
            warn!(%listen, error = %err, "daemon split dns tcp failed to bind");
            None
        }
    };

    let mut packet = [0_u8; 512];
    while !stop.load(Ordering::SeqCst) {
        match socket.recv_from(&mut packet) {
            Ok((len, peer)) => {
                let Some(response) =
                    build_split_dns_response(&packet[..len], &records, ttl_seconds)
                else {
                    if let Some(listener) = tcp_listener.as_ref() {
                        drain_split_dns_tcp(listener, &records, ttl_seconds);
                    }
                    continue;
                };
                if let Err(err) = socket.send_to(&response, peer) {
                    warn!(%peer, error = %err, "daemon split dns send failed");
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if let Some(listener) = tcp_listener.as_ref() {
                    drain_split_dns_tcp(listener, &records, ttl_seconds);
                }
                continue;
            }
            Err(err) => {
                warn!(%listen, error = %err, "daemon split dns receive failed");
            }
        }
        if let Some(listener) = tcp_listener.as_ref() {
            drain_split_dns_tcp(listener, &records, ttl_seconds);
        }
    }
}

fn drain_split_dns_tcp(listener: &TcpListener, records: &[SplitDnsRecord], ttl_seconds: u32) {
    loop {
        let (mut stream, peer) = match listener.accept() {
            Ok(connection) => connection,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return,
            Err(err) => {
                warn!(error = %err, "daemon split dns tcp accept failed");
                return;
            }
        };
        if let Err(err) = stream.set_read_timeout(Some(Duration::from_millis(500))) {
            warn!(%peer, error = %err, "daemon split dns tcp failed to set read timeout");
            continue;
        }
        if let Err(err) = stream.set_write_timeout(Some(Duration::from_millis(500))) {
            warn!(%peer, error = %err, "daemon split dns tcp failed to set write timeout");
            continue;
        }
        let mut len_buf = [0_u8; 2];
        if stream.read_exact(&mut len_buf).is_err() {
            continue;
        }
        let len = u16::from_be_bytes(len_buf) as usize;
        if len == 0 || len > 4096 {
            continue;
        }
        let mut request = vec![0_u8; len];
        if stream.read_exact(&mut request).is_err() {
            continue;
        }
        let Some(response) = build_split_dns_response(&request, records, ttl_seconds) else {
            continue;
        };
        let Ok(response_len) = u16::try_from(response.len()) else {
            continue;
        };
        if stream.write_all(&response_len.to_be_bytes()).is_err() {
            continue;
        }
        if let Err(err) = stream.write_all(&response) {
            warn!(%peer, error = %err, "daemon split dns tcp send failed");
        }
    }
}

fn normalize_dns_name(raw: &str) -> Option<String> {
    let name = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if name.is_empty()
        || name.len() > 253
        || name.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == b'-')
        })
    {
        None
    } else {
        Some(name)
    }
}

fn build_split_dns_response(
    request: &[u8],
    records: &[SplitDnsRecord],
    ttl_seconds: u32,
) -> Option<Vec<u8>> {
    if request.len() < 12 {
        return None;
    }
    let qdcount = u16::from_be_bytes([request[4], request[5]]);
    if qdcount == 0 {
        return None;
    }
    let (name, question_end) = parse_dns_question_name(request, 12)?;
    if question_end + 4 > request.len() {
        return None;
    }
    let qtype = u16::from_be_bytes([request[question_end], request[question_end + 1]]);
    let qclass = u16::from_be_bytes([request[question_end + 2], request[question_end + 3]]);
    let question_end = question_end + 4;
    let address = if qtype == 1 && qclass == 1 {
        records
            .iter()
            .find(|record| record.name == name)
            .map(|record| record.address)
    } else {
        None
    };

    let mut response = Vec::with_capacity(question_end + 32);
    response.extend_from_slice(&request[0..2]);
    response.extend_from_slice(&0x8180_u16.to_be_bytes());
    response.extend_from_slice(&1_u16.to_be_bytes());
    response.extend_from_slice(&(if address.is_some() { 1_u16 } else { 0_u16 }).to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&0_u16.to_be_bytes());
    response.extend_from_slice(&request[12..question_end]);
    if let Some(address) = address {
        response.extend_from_slice(&[0xC0, 0x0C]);
        response.extend_from_slice(&1_u16.to_be_bytes());
        response.extend_from_slice(&1_u16.to_be_bytes());
        response.extend_from_slice(&ttl_seconds.to_be_bytes());
        response.extend_from_slice(&4_u16.to_be_bytes());
        response.extend_from_slice(&address.octets());
    }
    Some(response)
}

fn parse_dns_question_name(packet: &[u8], mut offset: usize) -> Option<(String, usize)> {
    let mut labels = Vec::new();
    loop {
        let len = *packet.get(offset)? as usize;
        offset += 1;
        if len == 0 {
            break;
        }
        if len & 0xC0 != 0 || len > 63 {
            return None;
        }
        let end = offset.checked_add(len)?;
        let label = std::str::from_utf8(packet.get(offset..end)?).ok()?;
        labels.push(label.to_ascii_lowercase());
        offset = end;
    }
    normalize_dns_name(&labels.join(".")).map(|name| (name, offset))
}

fn runtime_cleanup_rules(config: Option<&DaemonRuntimeCleanupConfig>) -> Vec<RuntimeCleanupRule> {
    let configured = config
        .and_then(|cfg| cfg.rules.as_ref())
        .map(|rules| {
            rules
                .iter()
                .filter(|rule| rule.enabled.unwrap_or(true))
                .filter_map(|rule| {
                    let relative_path = safe_relative_runtime_path(&rule.path)?;
                    let max_age_minutes = rule
                        .max_age_minutes
                        .as_ref()
                        .and_then(FlexibleU64::value)
                        .unwrap_or(30)
                        .max(1);
                    Some(RuntimeCleanupRule {
                        relative_path,
                        max_age: Duration::from_secs(max_age_minutes.saturating_mul(60)),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !configured.is_empty() {
        return configured;
    }
    default_runtime_cleanup_rules()
}

fn default_runtime_cleanup_rules() -> Vec<RuntimeCleanupRule> {
    const MINUTE: u64 = 60;
    [
        ("runtime/tmp", 30 * MINUTE),
        ("runtime/audio/previews", 30 * MINUTE),
        ("runtime/audio/operator-breakin", 30 * MINUTE),
        ("runtime/cgen-probe", 30 * MINUTE),
        ("runtime/audio/playlist", 6 * 60 * MINUTE),
        ("runtime/audio/tts", 24 * 60 * MINUTE),
        ("runtime/ivr/cache", 24 * 60 * MINUTE),
        ("runtime/ivr/asr", 5 * MINUTE),
        ("runtime/audio/alerts", 7 * 24 * 60 * MINUTE),
        ("runtime/audio/easnet", 7 * 24 * 60 * MINUTE),
    ]
    .into_iter()
    .filter_map(|(path, seconds)| {
        Some(RuntimeCleanupRule {
            relative_path: safe_relative_runtime_path(path)?,
            max_age: Duration::from_secs(seconds),
        })
    })
    .collect()
}

fn safe_relative_runtime_path(raw: &str) -> Option<PathBuf> {
    let path = Path::new(raw.trim());
    if path.as_os_str().is_empty() || path.is_absolute() {
        return None;
    }
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => safe.push(part),
            std::path::Component::CurDir => {}
            _ => return None,
        }
    }
    if safe.as_os_str().is_empty() {
        None
    } else {
        Some(safe)
    }
}

fn run_runtime_cleanup_once(
    runtime_dir: &Path,
    rules: &[RuntimeCleanupRule],
    now: SystemTime,
) -> RuntimeCleanupStats {
    let mut total = RuntimeCleanupStats::default();
    for rule in rules {
        let Some(cutoff) = now.checked_sub(rule.max_age) else {
            continue;
        };
        let root = runtime_dir.join(&rule.relative_path);
        let stats = cleanup_runtime_tree(&root, cutoff);
        total.scanned_files = total.scanned_files.saturating_add(stats.scanned_files);
        total.removed_files = total.removed_files.saturating_add(stats.removed_files);
        total.removed_bytes = total.removed_bytes.saturating_add(stats.removed_bytes);
        total.error_count = total.error_count.saturating_add(stats.error_count);
    }
    total
}

fn cleanup_runtime_tree(root: &Path, cutoff: SystemTime) -> RuntimeCleanupStats {
    let Ok(entries) = fs::read_dir(root) else {
        return RuntimeCleanupStats::default();
    };
    let mut stats = RuntimeCleanupStats::default();
    for entry in entries {
        let Ok(entry) = entry else {
            stats.error_count = stats.error_count.saturating_add(1);
            continue;
        };
        let path = entry.path();
        let Ok(metadata) = entry.metadata() else {
            stats.error_count = stats.error_count.saturating_add(1);
            continue;
        };
        if metadata.is_dir() {
            let child_stats = cleanup_runtime_tree(&path, cutoff);
            stats.scanned_files = stats
                .scanned_files
                .saturating_add(child_stats.scanned_files);
            stats.removed_files = stats
                .removed_files
                .saturating_add(child_stats.removed_files);
            stats.removed_bytes = stats
                .removed_bytes
                .saturating_add(child_stats.removed_bytes);
            stats.error_count = stats.error_count.saturating_add(child_stats.error_count);
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        stats.scanned_files = stats.scanned_files.saturating_add(1);
        let Ok(modified) = metadata.modified() else {
            stats.error_count = stats.error_count.saturating_add(1);
            continue;
        };
        if modified > cutoff {
            continue;
        }
        let len = metadata.len();
        match fs::remove_file(&path) {
            Ok(()) => {
                stats.removed_files = stats.removed_files.saturating_add(1);
                stats.removed_bytes = stats.removed_bytes.saturating_add(len);
            }
            Err(_) => {
                stats.error_count = stats.error_count.saturating_add(1);
            }
        }
    }
    stats
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AlertQueueItem {
    id: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    feed_ids: Vec<String>,
    #[serde(default)]
    header: String,
    #[serde(default)]
    event: String,
    #[serde(default)]
    locations: Vec<String>,
    #[serde(default)]
    duration: String,
    #[serde(default)]
    callsign: String,
    #[serde(default)]
    tone: String,
    #[serde(default)]
    audio_path: String,
    #[serde(default)]
    manifest_path: String,
    #[serde(default)]
    format: String,
    #[serde(default)]
    sample_rate: u32,
    #[serde(default)]
    channels: u16,
    #[serde(default)]
    audio_bytes: u64,
    #[serde(default)]
    source: String,
    #[serde(default)]
    outputs: Vec<AlertOutputTarget>,
    #[serde(default)]
    claimed_at: Option<String>,
    #[serde(default)]
    played_at: Option<String>,
    #[serde(default)]
    failed_at: Option<String>,
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct AlertOutputTarget {
    #[serde(default)]
    feed_id: String,
    #[serde(default)]
    r#type: String,
    #[serde(default)]
    address: String,
    #[serde(default)]
    format: String,
    #[serde(default)]
    acodec: String,
    #[serde(default)]
    url: String,
}

fn alert_queue_loop(
    runtime_dir: PathBuf,
    interval_ms: u64,
    publisher: Sender<Value>,
    media_publisher: Sender<Value>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        if let Err(err) = process_alert_queue_once_with_stop(
            &runtime_dir,
            &publisher,
            &media_publisher,
            true,
            Some(&stop),
        ) {
            warn!("alert queue processing failed: {err}");
        }
        sleep_or_stopped(&stop, Duration::from_millis(interval_ms));
    }
}

#[cfg(test)]
fn process_alert_queue_once(
    runtime_dir: &Path,
    publisher: &Sender<Value>,
    sleep_for_audio: bool,
) -> Result<usize> {
    process_alert_queue_once_with_stop(runtime_dir, publisher, publisher, sleep_for_audio, None)
}

fn process_alert_queue_once_with_stop(
    runtime_dir: &Path,
    publisher: &Sender<Value>,
    media_publisher: &Sender<Value>,
    sleep_for_audio: bool,
    stop: Option<&AtomicBool>,
) -> Result<usize> {
    let queue_dir = runtime_dir.join(ALERT_QUEUE_DIR);
    let Ok(entries) = fs::read_dir(&queue_dir) else {
        return Ok(0);
    };
    let mut manifests = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            manifests.push(path);
        }
    }
    manifests.sort();

    let mut processed = 0usize;
    for manifest in manifests {
        if should_stop(stop) {
            break;
        }
        match process_alert_manifest(
            runtime_dir,
            &manifest,
            publisher,
            media_publisher,
            sleep_for_audio,
            stop,
        ) {
            Ok(true) => processed += 1,
            Ok(false) => {}
            Err(err) => {
                warn!(manifest = %manifest.display(), "failed to process alert queue item: {err}")
            }
        }
    }
    Ok(processed)
}

fn process_alert_manifest(
    runtime_dir: &Path,
    manifest: &Path,
    publisher: &Sender<Value>,
    media_publisher: &Sender<Value>,
    sleep_for_audio: bool,
    stop: Option<&AtomicBool>,
) -> Result<bool> {
    let raw = fs::read_to_string(manifest)
        .with_context(|| format!("failed to read alert manifest {}", manifest.display()))?;
    let mut item: AlertQueueItem = serde_json::from_str(&raw)
        .with_context(|| format!("failed to parse alert manifest {}", manifest.display()))?;
    if !is_pending_status(&item.status) {
        return Ok(false);
    }

    let audio_path = resolve_path(runtime_dir, Path::new(&item.audio_path));
    let audio_len = match fs::metadata(&audio_path) {
        Ok(metadata) => metadata.len(),
        Err(err) => {
            item.status = "failed".to_string();
            item.failed_at = Some(now_rfc3339());
            item.last_error = Some(format!("audio file is unavailable: {err}"));
            write_alert_manifest(manifest, &item)?;
            publish_alert_event(publisher, "alert.playout.failed", &item, Some(&audio_path));
            return Ok(true);
        }
    };
    item.status = "playing".to_string();
    item.claimed_at = Some(now_rfc3339());
    item.last_error = None;
    if item.audio_bytes == 0 {
        item.audio_bytes = audio_len;
    }
    write_alert_manifest(manifest, &item)?;
    publish_alert_event(publisher, "alert.playout.started", &item, Some(&audio_path));

    let delivered_in_realtime =
        match deliver_alert_outputs(&item, &audio_path, media_publisher, sleep_for_audio, stop) {
            Ok(value) => value,
            Err(err) => {
                item.status = "failed".to_string();
                item.failed_at = Some(now_rfc3339());
                item.last_error = Some(err.to_string());
                write_alert_manifest(manifest, &item)?;
                publish_alert_event(publisher, "alert.playout.failed", &item, Some(&audio_path));
                return Ok(true);
            }
        };

    if sleep_for_audio && !delivered_in_realtime {
        if let Some(duration) = audio_duration(&item, audio_len) {
            sleep_or_optional_stop(stop, duration);
        }
    }

    item.status = "played".to_string();
    item.played_at = Some(now_rfc3339());
    write_alert_manifest(manifest, &item)?;
    publish_alert_event(
        publisher,
        "alert.playout.completed",
        &item,
        Some(&audio_path),
    );
    Ok(true)
}

fn deliver_alert_outputs(
    item: &AlertQueueItem,
    audio_path: &Path,
    publisher: &Sender<Value>,
    sleep_for_audio: bool,
    stop: Option<&AtomicBool>,
) -> Result<bool> {
    if item.outputs.is_empty() {
        return Ok(false);
    }
    let mut delivered_in_realtime = false;
    for output in &item.outputs {
        match output.r#type.trim().to_ascii_lowercase().as_str() {
            "udp" => {
                deliver_udp_output(item, output, audio_path, sleep_for_audio, stop)?;
                delivered_in_realtime = delivered_in_realtime || sleep_for_audio;
            }
            "webrtc" | "media_bridge" => {
                deliver_media_bridge_output(
                    item,
                    output,
                    audio_path,
                    publisher,
                    sleep_for_audio,
                    stop,
                )?;
                delivered_in_realtime = delivered_in_realtime || sleep_for_audio;
            }
            "rtp" | "rtmp" | "srt" | "rtsp" | "icecast" | "stream" | "audio_device" => {
                bail!(
                    "output {} for feed {} is not supported by the alert queue worker yet",
                    output.r#type,
                    output.feed_id
                );
            }
            other => bail!(
                "output {other} for feed {} is not supported by the alert queue worker",
                output.feed_id
            ),
        }
    }
    Ok(delivered_in_realtime)
}

fn deliver_media_bridge_output(
    item: &AlertQueueItem,
    output: &AlertOutputTarget,
    audio_path: &Path,
    publisher: &Sender<Value>,
    sleep_for_audio: bool,
    stop: Option<&AtomicBool>,
) -> Result<()> {
    if !item.format.trim().eq_ignore_ascii_case("pcm_s16le") {
        bail!(
            "WebRTC output for feed {} requires pcm_s16le alert audio but manifest format is {}",
            output.feed_id,
            item.format
        );
    }
    let sample_rate = item.sample_rate.max(8_000);
    let channels = item.channels.max(1);
    let bytes_per_sample = 2usize;
    let frame_bytes = ((usize::try_from(sample_rate).unwrap_or(48_000)
        * usize::from(channels)
        * bytes_per_sample)
        / 50)
        .max(usize::from(channels) * bytes_per_sample);
    let raw = fs::read(audio_path)
        .with_context(|| format!("failed to read alert audio {}", audio_path.display()))?;
    let feed_id = if output.feed_id.trim().is_empty() {
        item.feed_ids.first().cloned().unwrap_or_default()
    } else {
        output.feed_id.clone()
    };
    if feed_id.trim().is_empty() {
        bail!("WebRTC output is missing feed_id");
    }
    for chunk in raw.chunks(frame_bytes) {
        if should_stop(stop) {
            break;
        }
        let duration_ms = pcm_chunk_duration_ms(chunk.len(), sample_rate, channels);
        let _ = publisher.send(json!({
            "type": "playout.pcm",
            "source": "daemon-alert-queue",
            "feed_id": feed_id,
            "data": {
                "feed_id": feed_id,
                "sample_rate": sample_rate,
                "channels": channels,
                "duration_ms": duration_ms,
                "pcm": encode_base64(chunk),
            },
            "timestamp_unix_ms": unix_now_ms(),
        }));
        if sleep_for_audio {
            sleep_or_optional_stop(stop, Duration::from_millis(u64::from(duration_ms.max(1))));
        }
    }
    Ok(())
}

fn pcm_chunk_duration_ms(bytes: usize, sample_rate: u32, channels: u16) -> u32 {
    let bytes_per_second = u64::from(sample_rate.max(1)) * u64::from(channels.max(1)) * 2;
    if bytes_per_second == 0 {
        return 20;
    }
    let millis = (u64::try_from(bytes).unwrap_or(0) * 1000).div_ceil(bytes_per_second);
    u32::try_from(millis.max(1)).unwrap_or(20)
}

fn encode_base64(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0b0011_1111) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn deliver_udp_output(
    item: &AlertQueueItem,
    output: &AlertOutputTarget,
    audio_path: &Path,
    sleep_for_audio: bool,
    stop: Option<&AtomicBool>,
) -> Result<()> {
    if !is_raw_pcm_output(output) {
        bail!(
            "udp output for feed {} requires raw PCM but is configured as format={} acodec={}",
            output.feed_id,
            output.format,
            output.acodec
        );
    }
    if output.address.trim().is_empty() {
        bail!(
            "udp output for feed {} is missing an address",
            output.feed_id
        );
    }
    let audio = fs::read(audio_path)
        .with_context(|| format!("failed to read alert audio {}", audio_path.display()))?;
    let socket = UdpSocket::bind(("0.0.0.0", 0)).context("failed to bind udp output socket")?;
    let chunk_size = udp_chunk_size(item);
    for chunk in audio.chunks(chunk_size) {
        if should_stop(stop) {
            break;
        }
        socket
            .send_to(chunk, output.address.trim())
            .with_context(|| format!("failed to send udp alert audio to {}", output.address))?;
        if sleep_for_audio {
            sleep_or_optional_stop(stop, Duration::from_millis(20));
        }
    }
    Ok(())
}

fn is_raw_pcm_output(output: &AlertOutputTarget) -> bool {
    let format = output.format.trim().to_ascii_lowercase();
    let acodec = output.acodec.trim().to_ascii_lowercase();
    matches!(format.as_str(), "" | "raw" | "pcm" | "pcm16le" | "s16le")
        && matches!(
            acodec.as_str(),
            "" | "raw" | "pcm" | "pcm_s16le" | "pcm16le" | "s16le"
        )
}

fn udp_chunk_size(item: &AlertQueueItem) -> usize {
    let sample_rate = usize::try_from(item.sample_rate).unwrap_or(48_000).max(1);
    let channels = usize::from(item.channels.max(1));
    let bytes_per_20ms = sample_rate
        .saturating_mul(channels)
        .saturating_mul(2)
        .checked_div(50)
        .unwrap_or(640);
    bytes_per_20ms.clamp(160, 1400)
}

fn write_alert_manifest(path: &Path, item: &AlertQueueItem) -> Result<()> {
    let raw = serde_json::to_vec_pretty(item).context("failed to encode alert manifest")?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, raw)
        .with_context(|| format!("failed to write alert manifest {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("failed to replace alert manifest {}", path.display()))?;
    Ok(())
}

fn publish_alert_event(
    publisher: &Sender<Value>,
    event_type: &str,
    item: &AlertQueueItem,
    audio_path: Option<&Path>,
) {
    let _ = publisher.send(json!({
        "type": event_type,
        "source": "daemon-alert-queue",
        "queue_id": &item.id,
        "feed_ids": &item.feed_ids,
        "event": &item.event,
        "header": &item.header,
        "status": &item.status,
        "audio_path": audio_path.map(|path| path.to_string_lossy().into_owned()),
        "sample_rate": item.sample_rate,
        "channels": item.channels,
        "duration_ms": audio_path
            .and_then(|path| fs::metadata(path).ok())
            .and_then(|metadata| audio_duration(item, metadata.len()))
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
        "timestamp_unix_ms": unix_now_ms(),
    }));
}

fn audio_duration(item: &AlertQueueItem, audio_len: u64) -> Option<Duration> {
    let sample_rate = u64::from(item.sample_rate);
    let channels = u64::from(item.channels.max(1));
    let bytes_per_sample = 2u64;
    let bytes_per_second = sample_rate
        .checked_mul(channels)?
        .checked_mul(bytes_per_sample)?;
    if bytes_per_second == 0 {
        return None;
    }
    let millis = audio_len.saturating_mul(1000) / bytes_per_second;
    Some(Duration::from_millis(millis.min(5 * 60 * 1000)))
}

fn is_pending_status(status: &str) -> bool {
    let normalized = status.trim().to_ascii_lowercase();
    normalized.is_empty() || normalized == "pending" || normalized == "queued"
}

fn should_stop(stop: Option<&AtomicBool>) -> bool {
    stop.is_some_and(|flag| flag.load(Ordering::SeqCst))
}

fn sleep_or_optional_stop(stop: Option<&AtomicBool>, duration: Duration) {
    if let Some(flag) = stop {
        sleep_or_stopped(flag, duration);
    } else {
        thread::sleep(duration);
    }
}

fn sleep_or_stopped(stop: &AtomicBool, duration: Duration) {
    let deadline = SystemTime::now()
        .checked_add(duration)
        .unwrap_or_else(SystemTime::now);
    while !stop.load(Ordering::SeqCst) {
        let now = SystemTime::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline
            .duration_since(now)
            .unwrap_or_else(|_| Duration::from_millis(0));
        thread::sleep(remaining.min(Duration::from_millis(50)));
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn publish_scheduled_package(publisher: &Sender<Value>, pkg_id: &str) {
    let _ = publisher.send(json!({
        "type": "scheduled_package",
        "source": "daemon-scheduler",
        "feed_id": "*",
        "pkg_id": pkg_id,
        "timestamp_unix_ms": unix_now_ms(),
    }));
}

fn decode_config_with_overlay<T>(path: &Path, app_dir: &Path, runtime_dir: &Path) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read daemon config {}", path.display()))?;
    let raw = expand_env_vars(&raw);
    let mut value: serde_yaml::Value = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse daemon config {}", path.display()))?;
    let settings_file = value
        .get("daemon_settings_file")
        .and_then(serde_yaml::Value::as_str)
        .filter(|raw| !raw.trim().is_empty())
        .unwrap_or(DEFAULT_SETTINGS_FILE);
    let settings_path = crate::runtime_dir::resolve_configured_runtime_path(
        app_dir,
        runtime_dir,
        Path::new(settings_file),
    );
    if settings_path.is_file() {
        let overlay_raw = fs::read_to_string(&settings_path).with_context(|| {
            format!(
                "failed to read daemon settings overlay {}",
                settings_path.display()
            )
        })?;
        let overlay_json: serde_json::Value =
            serde_json::from_str(&overlay_raw).with_context(|| {
                format!(
                    "failed to parse daemon settings overlay {}",
                    settings_path.display()
                )
            })?;
        merge_yaml(
            &mut value,
            serde_yaml::to_value(overlay_json)
                .context("failed to convert daemon settings overlay")?,
        );
    }
    serde_yaml::from_value(value).context("failed to decode daemon service configuration")
}

fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    match (base, overlay) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_yaml(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value;
        }
    }
}

fn expand_env_vars(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            let mut name = String::new();
            for next in chars.by_ref() {
                if next == '}' {
                    break;
                }
                name.push(next);
            }
            out.push_str(&std::env::var(name).unwrap_or_default());
            continue;
        }
        let mut name = String::new();
        while let Some(next) = chars.peek().copied() {
            if next == '_' || next.is_ascii_alphanumeric() {
                name.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if name.is_empty() {
            out.push('$');
        } else {
            out.push_str(&std::env::var(name).unwrap_or_default());
        }
    }
    out
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn unix_now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::mpsc;

    fn weekly_test_plan(targets: Vec<AutomationFeedTarget>) -> AutomationPlan {
        AutomationPlan {
            id: "required_weekly_test".to_string(),
            name: "Required Weekly Test".to_string(),
            description: String::new(),
            schedule: AutomationSchedule {
                weekdays: vec![3],
                hours: vec![12],
                minutes: vec![0],
                seconds: vec![0],
                weeks: vec![AutomationWeek {
                    week: 2,
                    event_override: String::new(),
                }],
                ..AutomationSchedule::default()
            },
            same: AutomationSame {
                enabled: true,
                event: "RWT".to_string(),
                originator: "WXR".to_string(),
                locations: Vec::new(),
                duration: "0015".to_string(),
                sender_id: String::new(),
                tone: "WXR".to_string(),
            },
            content: AutomationContent {
                text: String::new(),
                reader_id: "000".to_string(),
            },
            targets,
        }
    }

    fn dns_query(name: &str, qtype: u16) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.extend_from_slice(&0x1234_u16.to_be_bytes());
        packet.extend_from_slice(&0x0100_u16.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet.extend_from_slice(&0_u16.to_be_bytes());
        packet.extend_from_slice(&0_u16.to_be_bytes());
        packet.extend_from_slice(&0_u16.to_be_bytes());
        for label in name.split('.') {
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
        packet.extend_from_slice(&qtype.to_be_bytes());
        packet.extend_from_slice(&1_u16.to_be_bytes());
        packet
    }

    #[test]
    fn split_dns_answers_configured_a_record() {
        let records = vec![SplitDnsRecord {
            name: "internal.rai.blue".to_string(),
            address: Ipv4Addr::new(172, 16, 1, 30),
        }];
        let response = build_split_dns_response(&dns_query("internal.rai.blue", 1), &records, 60)
            .expect("response");

        assert_eq!(&response[0..2], &0x1234_u16.to_be_bytes());
        assert_eq!(&response[6..8], &1_u16.to_be_bytes());
        assert_eq!(&response[response.len() - 4..], &[172, 16, 1, 30]);
    }

    #[test]
    fn split_dns_returns_empty_answer_for_unknown_or_non_a_query() {
        let records = vec![SplitDnsRecord {
            name: "internal.rai.blue".to_string(),
            address: Ipv4Addr::new(172, 16, 1, 30),
        }];
        let unknown =
            build_split_dns_response(&dns_query("example.test", 1), &records, 60).expect("unknown");
        let aaaa = build_split_dns_response(&dns_query("internal.rai.blue", 28), &records, 60)
            .expect("aaaa");

        assert_eq!(&unknown[6..8], &0_u16.to_be_bytes());
        assert_eq!(&aaaa[6..8], &0_u16.to_be_bytes());
    }

    #[test]
    fn automation_schedule_uses_target_feed_local_time() {
        let toronto = AutomationFeedTarget {
            feed_id: "cwxr-on01".to_string(),
            timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
        };
        let regina = AutomationFeedTarget {
            feed_id: "cwxr-sk01".to_string(),
            timezone: ScheduleZone::Named("America/Regina".parse().expect("regina timezone")),
        };
        let plan = weekly_test_plan(vec![toronto.clone(), regina.clone()]);

        let toronto_noon = Utc
            .with_ymd_and_hms(2026, 7, 8, 16, 0, 0)
            .single()
            .expect("toronto noon");
        assert_eq!(
            plan.matching_event_for_target(toronto_noon, &toronto)
                .as_deref(),
            Some("RWT")
        );
        assert_eq!(plan.matching_event_for_target(toronto_noon, &regina), None);

        let regina_noon = Utc
            .with_ymd_and_hms(2026, 7, 8, 18, 0, 0)
            .single()
            .expect("regina noon");
        assert_eq!(
            plan.matching_event_for_target(regina_noon, &regina)
                .as_deref(),
            Some("RWT")
        );
    }

    #[test]
    fn automation_schedule_catches_up_after_the_exact_second_is_missed() {
        let toronto = AutomationFeedTarget {
            feed_id: "cwxr-on01".to_string(),
            timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
        };
        let plan = weekly_test_plan(vec![toronto.clone()]);
        let forty_five_seconds_after_noon = Utc
            .with_ymd_and_hms(2026, 7, 8, 16, 0, 45)
            .single()
            .expect("UTC datetime");

        let due = plan
            .due_for_target(forty_five_seconds_after_noon, &toronto)
            .expect("weekly test should catch up within the recovery window");

        assert_eq!(due.event, "RWT");
        assert_eq!(due.scheduled_for.to_rfc3339(), "2026-07-08T16:00:00+00:00");
    }

    #[test]
    fn automation_schedule_catches_up_after_a_service_outage() {
        let toronto = AutomationFeedTarget {
            feed_id: "cwxr-on01".to_string(),
            timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
        };
        let plan = weekly_test_plan(vec![toronto.clone()]);
        let six_hours_after_noon = Utc
            .with_ymd_and_hms(2026, 7, 8, 22, 0, 0)
            .single()
            .expect("UTC datetime");

        let due = plan
            .due_for_target(six_hours_after_noon, &toronto)
            .expect("weekly test should catch up after a same-day outage");

        assert_eq!(due.event, "RWT");
        assert_eq!(due.scheduled_for.to_rfc3339(), "2026-07-08T16:00:00+00:00");
    }

    #[test]
    fn automation_scheduled_time_discards_loop_subseconds() {
        let toronto = AutomationFeedTarget {
            feed_id: "cwxr-on01".to_string(),
            timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
        };
        let plan = weekly_test_plan(vec![toronto.clone()]);
        let first_loop = Utc
            .with_ymd_and_hms(2026, 7, 8, 16, 0, 1)
            .single()
            .expect("UTC datetime")
            .with_nanosecond(84_945_290)
            .expect("nanoseconds");
        let second_loop = first_loop
            .with_second(2)
            .and_then(|value| value.with_nanosecond(991_113_282))
            .expect("second loop time");

        let first_due = plan
            .due_for_target(first_loop, &toronto)
            .expect("first loop should find the weekly test");
        let second_due = plan
            .due_for_target(second_loop, &toronto)
            .expect("second loop should find the weekly test");

        assert_eq!(first_due.scheduled_for, second_due.scheduled_for);
        assert_eq!(
            first_due.scheduled_for.timestamp_millis(),
            1_783_526_400_000
        );
        assert_eq!(
            plan.dispatch_key(&toronto, first_due.scheduled_for),
            plan.dispatch_key(&toronto, second_due.scheduled_for)
        );
    }

    #[test]
    fn automation_catch_up_uses_each_target_timezone() {
        let toronto = AutomationFeedTarget {
            feed_id: "cwxr-on01".to_string(),
            timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
        };
        let regina = AutomationFeedTarget {
            feed_id: "cwxr-sk01".to_string(),
            timezone: ScheduleZone::Named("America/Regina".parse().expect("regina timezone")),
        };
        let plan = weekly_test_plan(vec![toronto.clone(), regina.clone()]);
        let shortly_after_toronto_noon = Utc
            .with_ymd_and_hms(2026, 7, 8, 16, 0, 45)
            .single()
            .expect("UTC datetime");

        assert!(plan
            .due_for_target(shortly_after_toronto_noon, &toronto)
            .is_some());
        assert!(plan
            .due_for_target(shortly_after_toronto_noon, &regina)
            .is_none());
    }

    #[test]
    fn automation_payload_includes_stable_dispatch_contract() {
        let target = AutomationFeedTarget {
            feed_id: "cwxr-on01".to_string(),
            timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
        };
        let plan = weekly_test_plan(vec![target.clone()]);
        let scheduled_for = Utc
            .with_ymd_and_hms(2026, 7, 8, 16, 0, 0)
            .single()
            .expect("scheduled time");
        let payload = plan.event_payload("RWT".to_string(), &target, scheduled_for);
        let data = payload
            .get("data")
            .and_then(Value::as_object)
            .expect("automation payload data");

        assert_eq!(
            data.get("dispatch_key").and_then(Value::as_str),
            Some("required_weekly_test:cwxr-on01:1783526400000")
        );
        assert_eq!(data.get("reader_id").and_then(Value::as_str), Some("000"));
    }

    #[test]
    fn automation_completion_event_decodes_acknowledgement() {
        let scheduled_for = Utc
            .with_ymd_and_hms(2026, 7, 8, 16, 0, 0)
            .single()
            .expect("scheduled time");
        let ack = automation_dispatch_ack(&json!({
            "type": "automation.dispatch.completed",
            "data": {
                "dispatch_key": "required_weekly_test:cwxr-on01:1783526400000",
                "scheduled_for": scheduled_for.to_rfc3339(),
            }
        }))
        .expect("automation acknowledgement");

        assert_eq!(ack.status, AutomationDispatchStatus::Completed);
        assert_eq!(
            ack.dispatch_key,
            "required_weekly_test:cwxr-on01:1783526400000"
        );
        assert_eq!(ack.scheduled_for, Some(scheduled_for));
    }

    #[test]
    fn automation_dispatch_ledger_prevents_a_restart_duplicate() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory
            .path()
            .join("state")
            .join("automation-dispatches.json");
        let scheduled_for = Utc::now();
        let mut ledger = AutomationDispatchLedger::default();
        ledger.record(
            "required_weekly_test:cwxr-on01:1783526400000".to_string(),
            scheduled_for,
        );
        ledger.save(&path).expect("save dispatch state");

        let restored = AutomationDispatchLedger::load(&path);
        assert!(restored.has_dispatched("required_weekly_test:cwxr-on01:1783526400000"));
    }

    #[test]
    fn wildcard_automation_targets_expand_to_configured_feeds() {
        let feeds = vec![
            AutomationFeed {
                id: "cwxr-on01".to_string(),
                timezone: ScheduleZone::Named("America/Toronto".parse().expect("toronto timezone")),
            },
            AutomationFeed {
                id: "cwxr-ab01".to_string(),
                timezone: ScheduleZone::Named(
                    "America/Edmonton".parse().expect("edmonton timezone"),
                ),
            },
        ];
        let targets = automation_targets(
            AutomationTargetXml {
                feeds: vec![AutomationFeedXml {
                    id: "*".to_string(),
                }],
            },
            &feeds,
        );

        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].feed_id, "cwxr-on01");
        assert_eq!(targets[0].timezone.name(), "America/Toronto");
        assert_eq!(targets[1].feed_id, "cwxr-ab01");
        assert_eq!(targets[1].timezone.name(), "America/Edmonton");
    }

    #[test]
    fn runtime_cleanup_removes_stale_files_under_configured_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target_dir = dir.path().join("runtime/audio/playlist/sk-0001");
        fs::create_dir_all(&target_dir).expect("target dir");
        let stale = target_dir.join("forecast.wav");
        fs::write(&stale, [1_u8, 2, 3, 4]).expect("stale file");
        fs::create_dir_all(dir.path().join("runtime/state")).expect("state dir");
        let state = dir.path().join("runtime/state/haze.db");
        fs::write(&state, [9_u8]).expect("state file");
        let rules = vec![RuntimeCleanupRule {
            relative_path: PathBuf::from("runtime/audio/playlist"),
            max_age: Duration::from_secs(60),
        }];

        let stats = run_runtime_cleanup_once(
            dir.path(),
            &rules,
            SystemTime::now() + Duration::from_secs(120),
        );

        assert_eq!(stats.removed_files, 1);
        assert_eq!(stats.removed_bytes, 4);
        assert!(!stale.exists());
        assert!(state.exists());
    }

    #[test]
    fn runtime_cleanup_keeps_fresh_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let target_dir = dir.path().join("runtime/audio/playlist/sk-0001");
        fs::create_dir_all(&target_dir).expect("target dir");
        let fresh = target_dir.join("current.wav");
        fs::write(&fresh, [1_u8, 2, 3, 4]).expect("fresh file");
        let rules = vec![RuntimeCleanupRule {
            relative_path: PathBuf::from("runtime/audio/playlist"),
            max_age: Duration::from_secs(60 * 60),
        }];

        let stats = run_runtime_cleanup_once(dir.path(), &rules, SystemTime::now());

        assert_eq!(stats.removed_files, 0);
        assert!(fresh.exists());
    }

    #[test]
    fn runtime_cleanup_maps_legacy_paths_into_separate_runtime_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let app_dir = dir.path().join("app");
        let runtime_dir = dir.path().join("data");
        let target_dir = runtime_dir.join("audio/playlist/cwxr-sk01");
        fs::create_dir_all(&app_dir).expect("app dir");
        fs::create_dir_all(&target_dir).expect("target dir");
        let stale = target_dir.join("forecast.wav");
        fs::write(&stale, [1_u8, 2, 3, 4]).expect("stale file");
        let configured_path = PathBuf::from("runtime/audio/playlist");
        let rules = vec![RuntimeCleanupRule {
            relative_path: crate::runtime_dir::configured_runtime_relative_path(
                &app_dir,
                &runtime_dir,
                &configured_path,
            ),
            max_age: Duration::from_secs(60),
        }];

        let stats = run_runtime_cleanup_once(
            &runtime_dir,
            &rules,
            SystemTime::now() + Duration::from_secs(120),
        );

        assert_eq!(rules[0].relative_path, PathBuf::from("audio/playlist"));
        assert_eq!(stats.removed_files, 1);
        assert!(!stale.exists());
        assert!(!runtime_dir.join("runtime").exists());
    }

    #[test]
    fn runtime_cleanup_rejects_unsafe_relative_paths() {
        assert!(safe_relative_runtime_path("../config.yaml").is_none());
        assert!(safe_relative_runtime_path("runtime/../config.yaml").is_none());
        assert!(safe_relative_runtime_path("").is_none());
        assert_eq!(
            safe_relative_runtime_path("runtime/audio/playlist"),
            Some(PathBuf::from("runtime/audio/playlist"))
        );
    }

    #[test]
    fn alert_queue_worker_marks_pending_item_played() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = dir.path();
        let queue_dir = runtime.join(ALERT_QUEUE_DIR);
        let audio_dir = runtime.join("runtime/audio/alerts");
        fs::create_dir_all(&queue_dir).expect("queue dir");
        fs::create_dir_all(&audio_dir).expect("audio dir");
        fs::write(audio_dir.join("test.pcm16le"), [0_u8, 1, 2, 3]).expect("audio");
        fs::write(
            queue_dir.join("test.json"),
            serde_json::to_vec_pretty(&json!({
                "id": "test",
                "type": "same_alert",
                "status": "pending",
                "feed_ids": ["sk-0001"],
                "header": "ZCZC-WXR-RWT-065522+0015-1661200-XLF322  -",
                "event": "RWT",
                "audio_path": "runtime/audio/alerts/test.pcm16le",
                "format": "raw",
                "sample_rate": 48000,
                "channels": 1,
                "audio_bytes": 4
            }))
            .expect("manifest json"),
        )
        .expect("manifest");
        let (tx, rx) = mpsc::channel();

        let processed = process_alert_queue_once(runtime, &tx, false).expect("process queue");

        assert_eq!(processed, 1);
        let updated_raw = fs::read_to_string(queue_dir.join("test.json")).expect("updated");
        let updated: AlertQueueItem = serde_json::from_str(&updated_raw).expect("updated json");
        assert_eq!(updated.status, "played");
        assert!(updated.claimed_at.is_some());
        assert!(updated.played_at.is_some());
        let events: Vec<Value> = rx.try_iter().collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "alert.playout.started");
        assert_eq!(events[1]["type"], "alert.playout.completed");
    }

    #[test]
    fn alert_queue_worker_marks_missing_audio_failed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = dir.path();
        let queue_dir = runtime.join(ALERT_QUEUE_DIR);
        fs::create_dir_all(&queue_dir).expect("queue dir");
        fs::write(
            queue_dir.join("missing.json"),
            serde_json::to_vec_pretty(&json!({
                "id": "missing",
                "type": "same_alert",
                "status": "pending",
                "feed_ids": ["sk-0001"],
                "header": "ZCZC-WXR-RWT-065522+0015-1661200-XLF322  -",
                "event": "RWT",
                "audio_path": "runtime/audio/alerts/missing.pcm16le",
                "format": "raw",
                "sample_rate": 48000,
                "channels": 1
            }))
            .expect("manifest json"),
        )
        .expect("manifest");
        let (tx, rx) = mpsc::channel();

        let processed = process_alert_queue_once(runtime, &tx, false).expect("process queue");

        assert_eq!(processed, 1);
        let updated_raw = fs::read_to_string(queue_dir.join("missing.json")).expect("updated");
        let updated: AlertQueueItem = serde_json::from_str(&updated_raw).expect("updated json");
        assert_eq!(updated.status, "failed");
        assert!(updated.failed_at.is_some());
        assert!(updated.last_error.is_some());
        let events: Vec<Value> = rx.try_iter().collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "alert.playout.failed");
    }

    #[test]
    fn alert_queue_worker_sends_raw_udp_output() {
        let receiver = UdpSocket::bind(("127.0.0.1", 0)).expect("receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("timeout");
        let address = receiver.local_addr().expect("local addr").to_string();
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = dir.path();
        let queue_dir = runtime.join(ALERT_QUEUE_DIR);
        let audio_dir = runtime.join("runtime/audio/alerts");
        fs::create_dir_all(&queue_dir).expect("queue dir");
        fs::create_dir_all(&audio_dir).expect("audio dir");
        fs::write(audio_dir.join("udp.pcm16le"), [1_u8, 2, 3, 4, 5, 6]).expect("audio");
        fs::write(
            queue_dir.join("udp.json"),
            serde_json::to_vec_pretty(&json!({
                "id": "udp",
                "type": "same_alert",
                "status": "pending",
                "feed_ids": ["sk-0001"],
                "header": "ZCZC-WXR-RWT-065522+0015-1661200-XLF322  -",
                "event": "RWT",
                "audio_path": "runtime/audio/alerts/udp.pcm16le",
                "format": "raw",
                "sample_rate": 48000,
                "channels": 1,
                "outputs": [{
                    "feed_id": "sk-0001",
                    "type": "udp",
                    "address": address,
                    "format": "raw",
                    "acodec": "pcm_s16le"
                }]
            }))
            .expect("manifest json"),
        )
        .expect("manifest");
        let (tx, _rx) = mpsc::channel();

        let processed = process_alert_queue_once(runtime, &tx, false).expect("process queue");

        assert_eq!(processed, 1);
        let mut buf = [0_u8; 32];
        let len = receiver.recv(&mut buf).expect("udp packet");
        assert_eq!(&buf[..len], &[1_u8, 2, 3, 4, 5, 6]);
        let updated_raw = fs::read_to_string(queue_dir.join("udp.json")).expect("updated");
        let updated: AlertQueueItem = serde_json::from_str(&updated_raw).expect("updated json");
        assert_eq!(updated.status, "played");
    }

    #[test]
    fn alert_queue_worker_publishes_webrtc_pcm_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = dir.path();
        let queue_dir = runtime.join(ALERT_QUEUE_DIR);
        let audio_dir = runtime.join("runtime/audio/alerts");
        fs::create_dir_all(&queue_dir).expect("queue dir");
        fs::create_dir_all(&audio_dir).expect("audio dir");
        fs::write(audio_dir.join("webrtc.pcm16le"), [1_u8, 2, 3, 4, 5, 6]).expect("audio");
        fs::write(
            queue_dir.join("webrtc.json"),
            serde_json::to_vec_pretty(&json!({
                "id": "webrtc",
                "type": "same_alert",
                "status": "pending",
                "feed_ids": ["CAP-IT-ALL"],
                "header": "ZCZC-EAS-SVR-000000+0015-1661200-CAPALL  -",
                "event": "SVR",
                "audio_path": "runtime/audio/alerts/webrtc.pcm16le",
                "format": "pcm_s16le",
                "sample_rate": 48000,
                "channels": 1,
                "audio_bytes": 6,
                "outputs": [{
                    "feed_id": "CAP-IT-ALL",
                    "type": "webrtc",
                    "format": "pcm_s16le"
                }]
            }))
            .expect("manifest json"),
        )
        .expect("manifest");
        let (tx, rx) = mpsc::channel();

        let processed = process_alert_queue_once(runtime, &tx, false).expect("process queue");

        assert_eq!(processed, 1);
        let events: Vec<Value> = rx.try_iter().collect();
        assert_eq!(events[0]["type"], "alert.playout.started");
        let pcm = events
            .iter()
            .find(|event| event["type"] == "playout.pcm")
            .expect("playout pcm event");
        assert_eq!(pcm["feed_id"], "CAP-IT-ALL");
        assert_eq!(pcm["data"]["feed_id"], "CAP-IT-ALL");
        assert_eq!(pcm["data"]["sample_rate"], 48000);
        assert_eq!(pcm["data"]["channels"], 1);
        assert_eq!(pcm["data"]["pcm"], "AQIDBAUG");
        assert_eq!(
            events.last().expect("last event")["type"],
            "alert.playout.completed"
        );
    }

    #[test]
    fn alert_queue_worker_fails_unsupported_udp_container() {
        let dir = tempfile::tempdir().expect("tempdir");
        let runtime = dir.path();
        let queue_dir = runtime.join(ALERT_QUEUE_DIR);
        let audio_dir = runtime.join("runtime/audio/alerts");
        fs::create_dir_all(&queue_dir).expect("queue dir");
        fs::create_dir_all(&audio_dir).expect("audio dir");
        fs::write(audio_dir.join("mpegts.pcm16le"), [0_u8, 1, 2, 3]).expect("audio");
        fs::write(
            queue_dir.join("mpegts.json"),
            serde_json::to_vec_pretty(&json!({
                "id": "mpegts",
                "type": "same_alert",
                "status": "pending",
                "feed_ids": ["sk-0001"],
                "header": "ZCZC-WXR-RWT-065522+0015-1661200-XLF322  -",
                "event": "RWT",
                "audio_path": "runtime/audio/alerts/mpegts.pcm16le",
                "format": "raw",
                "sample_rate": 48000,
                "channels": 1,
                "outputs": [{
                    "feed_id": "sk-0001",
                    "type": "udp",
                    "address": "127.0.0.1:9",
                    "format": "mpegts",
                    "acodec": "libopus"
                }]
            }))
            .expect("manifest json"),
        )
        .expect("manifest");
        let (tx, rx) = mpsc::channel();

        let processed = process_alert_queue_once(runtime, &tx, false).expect("process queue");

        assert_eq!(processed, 1);
        let updated_raw = fs::read_to_string(queue_dir.join("mpegts.json")).expect("updated");
        let updated: AlertQueueItem = serde_json::from_str(&updated_raw).expect("updated json");
        assert_eq!(updated.status, "failed");
        assert!(updated
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("requires raw PCM"));
        let events: Vec<Value> = rx.try_iter().collect();
        assert_eq!(
            events.last().expect("event")["type"],
            "alert.playout.failed"
        );
    }
}
