use super::*;

pub(crate) fn route_document(
    alert: &Alert,
    delivery_id: &str,
    received_at: &str,
    feeds: &[FeedConfig],
    state: &mut RouterState,
) -> Vec<AlertDispatch> {
    let mut dispatches = Vec::new();
    for feed in feeds.iter().filter(|feed| feed.is_enabled()) {
        prune_expired_products(state, &feed.id, Utc::now());
        if !source_enabled(feed, alert) {
            continue;
        }
        if is_cancel(alert) {
            dispatches.extend(route_cancellation(
                alert,
                delivery_id,
                received_at,
                feed,
                state,
            ));
            continue;
        }
        if !routine_alert_allowed(alert) {
            continue;
        }

        let provider = provider_for(feed, alert);
        let groups = info_groups(alert);
        let mut current_group_ids = HashSet::new();
        let references = referenced_alert_ids(alert);
        let target_ids = if references.is_empty() {
            BTreeSet::from([alert.identifier.clone()])
        } else {
            references.into_iter().collect::<BTreeSet<_>>()
        };
        let active = state.active.entry(feed.id.clone()).or_default();

        for group in groups {
            let Some(info) = choose_info(&group.infos, &feed.language()) else {
                continue;
            };
            if info_expired(info) || !filter_allows(&provider.filter, alert, info) {
                continue;
            }
            let hazard_prior_locations = active
                .iter()
                .filter(|product| {
                    product_matches_targets(product, &target_ids)
                        && product.hazard_key == group.hazard_key
                })
                .flat_map(|product| product.locations.iter().cloned())
                .collect::<BTreeSet<_>>();
            let explicit_new = explicit_new_locations(info);
            let mut current_locations = feed_locations(feed, &provider.filter, info);
            if current_locations.is_empty()
                && alert.message_type.eq_ignore_ascii_case("update")
                && all_location_codes(info).is_empty()
                && explicit_new.is_empty()
            {
                current_locations.extend(hazard_prior_locations.iter().cloned());
            }
            if current_locations.is_empty() {
                continue;
            }

            current_group_ids.insert(group.id.clone());
            let newly_active_locations = if !explicit_new.is_empty() {
                intersect_locations(&current_locations, &explicit_new)
            } else {
                current_locations
                    .iter()
                    .filter(|location| !hazard_prior_locations.contains(*location))
                    .cloned()
                    .collect()
            };
            let same_event = same_event_code(alert, info);
            let same_locations = same_codes(alert, info, feed, &newly_active_locations);
            let priority = !newly_active_locations.is_empty()
                && priority_eligible(alert, info, &same_event)
                && feed.same_enabled()
                && !same_event.is_empty()
                && !same_locations.is_empty();
            let kind = if priority {
                DispatchKind::Priority
            } else {
                DispatchKind::Routine
            };
            let title = nonempty(&info.headline, &info.event, "Weather alert");
            let text = alert_text(alert, info, &current_locations);
            let parent_alert_id = active
                .iter()
                .find(|product| product_matches_targets(product, &target_ids))
                .map(|product| product.parent_alert_id.clone())
                .unwrap_or_else(|| alert.identifier.clone());
            let mut lineage_ids =
                BTreeSet::from([parent_alert_id.clone(), alert.identifier.clone()]);
            lineage_ids.extend(target_ids.iter().cloned());
            for product in active
                .iter()
                .filter(|product| product.parent_alert_id == parent_alert_id)
            {
                lineage_ids.extend(product.lineage_ids.iter().cloned());
                lineage_ids.insert(product.alert_id.clone());
            }
            let dispatch = make_dispatch(
                delivery_id,
                alert,
                info,
                feed,
                group.id.clone(),
                parent_alert_id.clone(),
                kind,
                current_locations.clone(),
                newly_active_locations.clone(),
                same_event.clone(),
                same_locations.clone(),
                priority,
                title.clone(),
                text.clone(),
                received_at,
            );
            dispatches.push(dispatch);

            active.retain(|product| {
                !(product_matches_targets(product, &target_ids)
                    && product.hazard_key == group.hazard_key)
            });
            active.push(ActiveProduct {
                parent_alert_id,
                alert_id: alert.identifier.clone(),
                lineage_ids: lineage_ids.into_iter().collect(),
                info_group_id: group.id,
                hazard_key: group.hazard_key,
                locations: current_locations,
                expires_at: info.expires.clone(),
                same_locations,
                title,
                text,
            });
        }

        if alert.message_type.eq_ignore_ascii_case("update") {
            active.retain(|product| {
                !(product_matches_targets(product, &target_ids)
                    && !current_group_ids.contains(&product.info_group_id))
            });
        }
    }
    dispatches
}

fn product_matches_targets(product: &ActiveProduct, target_ids: &BTreeSet<String>) -> bool {
    target_ids.contains(&product.parent_alert_id)
        || target_ids.contains(&product.alert_id)
        || product.lineage_ids.iter().any(|id| target_ids.contains(id))
}

fn prune_expired_products(state: &mut RouterState, feed_id: &str, now: DateTime<Utc>) {
    if let Some(active) = state.active.get_mut(feed_id) {
        active.retain(|product| {
            parse_cap_time(&product.expires_at).map_or(true, |expires| expires > now)
        });
    }
}

fn info_expired(info: &AlertInfo) -> bool {
    parse_cap_time(&info.expires).is_some_and(|expires| expires <= Utc::now())
}

fn route_cancellation(
    alert: &Alert,
    delivery_id: &str,
    received_at: &str,
    feed: &FeedConfig,
    state: &mut RouterState,
) -> Vec<AlertDispatch> {
    let targets = referenced_alert_ids(alert);
    let targets = if targets.is_empty() {
        BTreeSet::from([alert.identifier.clone()])
    } else {
        targets.into_iter().collect::<BTreeSet<_>>()
    };
    let active = state.active.entry(feed.id.clone()).or_default();
    let mut cancelled_ids = targets.clone();
    for product in active
        .iter()
        .filter(|product| product_matches_targets(product, &targets))
    {
        cancelled_ids.insert(product.parent_alert_id.clone());
        cancelled_ids.insert(product.alert_id.clone());
        cancelled_ids.extend(product.lineage_ids.iter().cloned());
    }
    let mut removed = Vec::new();
    active.retain(|product| {
        if product_matches_targets(product, &cancelled_ids) {
            removed.push(product.clone());
            false
        } else {
            true
        }
    });
    state.outbox.retain(|_, dispatch| {
        !(dispatch.feed_id == feed.id
            && (cancelled_ids.contains(&dispatch.alert_id)
                || cancelled_ids.contains(&dispatch.parent_alert_id)))
    });
    removed
        .into_iter()
        .map(|product| {
            let mut cancelled_alert_ids =
                BTreeSet::from([product.parent_alert_id.clone(), product.alert_id.clone()]);
            cancelled_alert_ids.extend(product.lineage_ids.iter().cloned());
            let info = choose_info(&alert.infos, &feed.language())
                .cloned()
                .unwrap_or_default();
            let title = nonempty(&info.headline, &info.event, &product.title);
            let text = if !info.headline.trim().is_empty()
                || !info.description.trim().is_empty()
                || !info.instruction.trim().is_empty()
            {
                alert_text(alert, &info, &product.locations)
            } else {
                format!(
                    "{} has ended. {}, {}",
                    product.title,
                    product.text,
                    product.locations.join(", ")
                )
            };
            let mut dispatch = make_dispatch(
                delivery_id,
                alert,
                &info,
                feed,
                product.info_group_id,
                product.parent_alert_id,
                DispatchKind::Cancellation,
                product.locations,
                Vec::new(),
                String::new(),
                Vec::new(),
                false,
                title,
                text,
                received_at,
            );
            dispatch.cancelled_alert_ids = cancelled_alert_ids.into_iter().collect();
            dispatch
        })
        .collect()
}

#[derive(Debug)]
struct InfoGroup {
    id: String,
    hazard_key: String,
    infos: Vec<AlertInfo>,
}

fn info_groups(alert: &Alert) -> Vec<InfoGroup> {
    let mut groups = BTreeMap::<String, InfoGroup>::new();
    for info in &alert.infos {
        let codes = all_location_codes(info);
        let mut category = info
            .category
            .iter()
            .map(|value| value.trim().to_ascii_lowercase())
            .collect::<Vec<_>>();
        category.sort();
        let event_identity = match same_event_code(alert, info) {
            code if !code.is_empty() => format!("same:{code}"),
            _ => format!("event:{}", info.event.trim().to_ascii_lowercase()),
        };
        let hazard_key = format!("{event_identity}|{}", category.join("|"));
        let scope = codes.into_iter().collect::<Vec<_>>().join(",");
        let key = format!("{hazard_key}|{scope}");
        let id = stable_id(&key);
        let group = groups.entry(key).or_insert_with(|| InfoGroup {
            id,
            hazard_key,
            infos: Vec::new(),
        });
        group.infos.push(info.clone());
    }
    groups.into_values().collect()
}

fn all_location_codes(info: &AlertInfo) -> BTreeSet<String> {
    let mut codes = BTreeSet::new();
    for area in &info.areas {
        for geocode in &area.geocodes {
            let name = geocode.name.to_ascii_lowercase();
            if name.contains(":dlc:") || name.contains("threat") {
                continue;
            }
            let code = normalize_location(&geocode.value);
            if !code.is_empty() {
                codes.insert(code);
            }
        }
    }
    codes
}

fn feed_locations(feed: &FeedConfig, filter: &AlertFilterConfig, info: &AlertInfo) -> Vec<String> {
    let codes = all_location_codes(info);
    if !xml_bool(filter.use_feed_locations.as_deref(), true) {
        return if codes.is_empty() {
            vec!["*".to_string()]
        } else {
            codes.into_iter().collect()
        };
    }
    let coverage = feed
        .locations
        .coverage
        .regions
        .iter()
        .map(|region| normalize_location(&region.id))
        .filter(|code| !code.is_empty())
        .collect::<HashSet<_>>();
    if coverage.is_empty() {
        return if feed.alert_covers_all_locations() {
            if codes.is_empty() {
                vec!["*".to_string()]
            } else {
                codes.into_iter().collect()
            }
        } else {
            Vec::new()
        };
    }
    let selected = codes
        .iter()
        .filter(|code| coverage.contains(*code))
        .cloned()
        .collect::<BTreeSet<_>>();
    if selected.is_empty() {
        return Vec::new();
    }
    selected.into_iter().collect()
}

fn explicit_new_locations(info: &AlertInfo) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    for parameter in &info.parameters {
        let name = parameter.name.to_ascii_lowercase();
        if name.contains("newly_active_areas") || name.contains("newly active") {
            for value in parameter.value.split([',', ';', '|', '\n', '\r', '\t']) {
                let code = normalize_location(value);
                if !code.is_empty() {
                    result.insert(code);
                }
            }
        }
    }
    for area in &info.areas {
        if area.threat_status.eq_ignore_ascii_case("issued") {
            for geocode in &area.geocodes {
                let name = geocode.name.to_ascii_lowercase();
                if name.contains(":dlc:") || name.contains("threat") {
                    continue;
                }
                let code = normalize_location(&geocode.value);
                if !code.is_empty() {
                    result.insert(code);
                }
            }
        }
    }
    result
}

fn intersect_locations(locations: &[String], selected: &BTreeSet<String>) -> Vec<String> {
    locations
        .iter()
        .filter(|location| selected.contains(*location))
        .cloned()
        .collect()
}

fn same_codes(
    alert: &Alert,
    info: &AlertInfo,
    feed: &FeedConfig,
    locations: &[String],
) -> Vec<String> {
    let mut same = BTreeSet::new();
    for area in &info.areas {
        let area_locations = area
            .geocodes
            .iter()
            .filter(|geocode| {
                let name = geocode.name.to_ascii_lowercase();
                !name.contains(":dlc:") && !name.contains("threat")
            })
            .map(|geocode| normalize_location(&geocode.value))
            .filter(|code| !code.is_empty())
            .collect::<BTreeSet<_>>();
        let newly_covered = area_locations
            .iter()
            .filter(|location| locations.contains(location))
            .cloned()
            .collect::<BTreeSet<_>>();
        if newly_covered.is_empty() {
            continue;
        }
        let same_codes = area
            .geocodes
            .iter()
            .filter_map(|geocode| {
                let name = geocode.name.to_ascii_lowercase();
                let code = normalize_location(&geocode.value);
                ((name.contains("same")
                    || name.contains("eas")
                    || name.contains("clc")
                    || name.contains("fips"))
                    && is_same_code(&code))
                .then_some(code)
            })
            .collect::<BTreeSet<_>>();
        let exact_new_codes = same_codes
            .intersection(&newly_covered)
            .cloned()
            .collect::<BTreeSet<_>>();
        if !exact_new_codes.is_empty() {
            same.extend(exact_new_codes);
        } else if same_codes.len() == 1 {
            // A single SAME code on an area that maps to a newly covered UGC is an
            // unambiguous area-level mapping.
            same.extend(same_codes);
        }
    }
    if same.is_empty() {
        for region in &feed.locations.coverage.regions {
            let code = normalize_location(&region.id);
            if is_same_code(&code) && locations.contains(&code) {
                same.insert(code);
            }
        }
    }
    if same.is_empty() && feed.alert_covers_all_locations() && alert.infos.len() == 1 {
        for code in locations {
            if is_same_code(code) {
                same.insert(code.clone());
            }
        }
    }
    same.into_iter().take(31).collect()
}

fn source_enabled(feed: &FeedConfig, alert: &Alert) -> bool {
    let source = detect_source(alert);
    let Some(alerts) = &feed.alerts else {
        return false;
    };
    match source.as_str() {
        "nws" => xml_bool(alerts.nws_cap.enabled.as_deref(), false),
        _ => xml_bool(alerts.cap_cp.enabled.as_deref(), true),
    }
}

fn provider_for<'a>(feed: &'a FeedConfig, alert: &Alert) -> &'a FeedAlertProviderConfig {
    let source = detect_source(alert);
    let alerts = feed.alerts.as_ref();
    match source.as_str() {
        "nws" => alerts
            .map(|value| &value.nws_cap)
            .unwrap_or_else(|| unreachable!()),
        _ => alerts
            .map(|value| &value.cap_cp)
            .unwrap_or_else(|| unreachable!()),
    }
}

fn detect_source(alert: &Alert) -> String {
    if alert
        .infos
        .iter()
        .flat_map(|info| &info.parameters)
        .any(|parameter| {
            parameter.name.starts_with("layer:EC-MSC-SMC")
                || parameter.name.to_ascii_lowercase().contains("cap-cp")
        })
    {
        return "eccc".to_string();
    }
    let sender = alert.sender.to_ascii_lowercase();
    if sender.contains("canada") || sender.contains("cap-pac") {
        return "eccc".to_string();
    }
    if sender.contains("weather.gov") || sender.contains("nws") || sender.contains("noaa") {
        return "nws".to_string();
    }
    "generic".to_string()
}

fn filter_allows(filter: &AlertFilterConfig, alert: &Alert, info: &AlertInfo) -> bool {
    if list_matches(&filter.blocklist, alert, info) {
        return false;
    }
    list_empty(&filter.allowlist) || list_allows(&filter.allowlist, alert, info)
}

fn list_allows(list: &AlertFilterListConfig, alert: &Alert, info: &AlertInfo) -> bool {
    (list.severities.is_empty() || text_in_list(&info.severity, &list.severities))
        && (list.urgencies.is_empty() || text_in_list(&info.urgency, &list.urgencies))
        && (list.certainties.is_empty() || text_in_list(&info.certainty, &list.certainties))
        && (list.message_types.is_empty() || text_in_list(&alert.message_type, &list.message_types))
        && (list.events.is_empty() || event_matches(info, &list.events))
        && (list.naads_events.is_empty() || event_matches(info, &list.naads_events))
        && list.others.iter().all(|other| {
            info.parameters.iter().any(|parameter| {
                parameter.name.eq_ignore_ascii_case(&other.value_name)
                    && parameter.value.eq_ignore_ascii_case(&other.value)
            })
        })
}

fn list_matches(list: &AlertFilterListConfig, alert: &Alert, info: &AlertInfo) -> bool {
    (!list.severities.is_empty() && text_in_list(&info.severity, &list.severities))
        || (!list.urgencies.is_empty() && text_in_list(&info.urgency, &list.urgencies))
        || (!list.certainties.is_empty() && text_in_list(&info.certainty, &list.certainties))
        || (!list.message_types.is_empty()
            && text_in_list(&alert.message_type, &list.message_types))
        || (!list.events.is_empty() && event_matches(info, &list.events))
        || (!list.naads_events.is_empty() && event_matches(info, &list.naads_events))
        || list.others.iter().any(|other| {
            info.parameters.iter().any(|parameter| {
                parameter.name.eq_ignore_ascii_case(&other.value_name)
                    && parameter.value.eq_ignore_ascii_case(&other.value)
            })
        })
}

fn list_empty(list: &AlertFilterListConfig) -> bool {
    list.severities.is_empty()
        && list.urgencies.is_empty()
        && list.certainties.is_empty()
        && list.message_types.is_empty()
        && list.events.is_empty()
        && list.naads_events.is_empty()
        && list.others.is_empty()
}

fn event_matches(info: &AlertInfo, wanted: &[String]) -> bool {
    text_in_list(&info.event, wanted)
        || text_in_list(&info.headline, wanted)
        || info
            .event_codes
            .iter()
            .any(|code| text_in_list(&code.value, wanted))
}

fn text_in_list(value: &str, wanted: &[String]) -> bool {
    wanted
        .iter()
        .any(|item| item.trim().eq_ignore_ascii_case(value.trim()))
}

fn routine_alert_allowed(alert: &Alert) -> bool {
    if ["test", "exercise", "draft"]
        .iter()
        .any(|status| alert.status.eq_ignore_ascii_case(status))
    {
        return false;
    }
    let text = alert
        .infos
        .iter()
        .flat_map(|info| [&info.event, &info.headline])
        .map(|value| value.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    ![
        "test message",
        "practice demo",
        "practice/demo",
        "required weekly test",
        "required monthly test",
    ]
    .iter()
    .any(|marker| text.contains(marker))
}

fn priority_eligible(alert: &Alert, info: &AlertInfo, same_event: &str) -> bool {
    if is_cancel(alert) || !is_fresh(alert, info, same_event) {
        return false;
    }
    let explicit = info.parameters.iter().any(|parameter| {
        let name = parameter.name.to_ascii_lowercase();
        name.contains("broadcast_immediate")
            && matches!(
                parameter.value.trim().to_ascii_lowercase().as_str(),
                "true" | "yes" | "1"
            )
    });
    explicit
        || (matches!(
            info.severity.to_ascii_lowercase().as_str(),
            "severe" | "extreme"
        ) && matches!(
            info.urgency.to_ascii_lowercase().as_str(),
            "immediate" | "expected"
        ) && matches!(
            info.certainty.to_ascii_lowercase().as_str(),
            "observed" | "likely"
        ))
}

fn is_fresh(alert: &Alert, info: &AlertInfo, same_event: &str) -> bool {
    let anchor = [
        alert.sent.as_str(),
        info.effective.as_str(),
        info.onset.as_str(),
    ]
    .into_iter()
    .find_map(parse_cap_time);
    let Some(anchor) = anchor else {
        return true;
    };
    let limit = if matches!(same_event, "SVR" | "TOR") {
        Duration::minutes(30)
    } else {
        Duration::hours(1)
    };
    let now = Utc::now();
    now < anchor || now.signed_duration_since(anchor) <= limit
}

fn same_event_code(alert: &Alert, info: &AlertInfo) -> String {
    for event_code in &info.event_codes {
        if matches!(
            event_code.name.trim().to_ascii_uppercase().as_str(),
            "SAME" | "EAS"
        ) && event_code.value.len() == 3
            && event_code
                .value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric())
        {
            return event_code.value.to_ascii_uppercase();
        }
    }
    let event = format!("{} {}", info.event, info.headline).to_ascii_lowercase();
    let pairs = [
        ("tornado warning", "TOR"),
        ("tornado watch", "TOA"),
        ("severe thunderstorm warning", "SVR"),
        ("severe thunderstorm watch", "SVA"),
        ("flash flood warning", "FFW"),
        ("flood warning", "FLW"),
        ("winter storm warning", "WSW"),
        ("blizzard warning", "BZW"),
        ("hurricane warning", "HUW"),
        ("high wind warning", "HWW"),
        ("extreme wind warning", "EWW"),
        ("civil emergency", "CEM"),
    ];
    let _ = alert;
    pairs
        .iter()
        .find_map(|(label, code)| event.contains(label).then(|| (*code).to_string()))
        .unwrap_or_default()
}

fn is_cancel(alert: &Alert) -> bool {
    alert.message_type.eq_ignore_ascii_case("cancel")
}

fn referenced_alert_ids(alert: &Alert) -> HashSet<String> {
    alert
        .references
        .split_whitespace()
        .filter_map(|reference| reference.split(',').nth(1))
        .map(str::trim)
        .filter(|identifier| !identifier.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn choose_info<'a>(infos: &'a [AlertInfo], language: &str) -> Option<&'a AlertInfo> {
    let language = language.to_ascii_lowercase();
    let short = language.split('-').next().unwrap_or(&language);
    infos
        .iter()
        .find(|info| {
            let info_language = info.language.to_ascii_lowercase();
            info_language == language
                || info_language == short
                || (short == "en" && info_language.starts_with("en"))
        })
        .or_else(|| {
            infos
                .iter()
                .find(|info| info.language.to_ascii_lowercase().starts_with("en"))
        })
        .or_else(|| infos.first())
}

fn alert_text(alert: &Alert, info: &AlertInfo, locations: &[String]) -> String {
    let mut sentences = Vec::new();
    let headline = nonempty(&info.headline, &info.event, "Weather alert");
    let areas = info
        .areas
        .iter()
        .filter(|area| !area.description.trim().is_empty())
        .map(|area| area.description.trim())
        .collect::<Vec<_>>();
    let area_text = if areas.is_empty() {
        locations.join(", ")
    } else {
        areas.join(", ")
    };
    let source = info.sender_name.trim();
    if !source.is_empty() {
        sentences.push(format!("{source} has issued a {headline} for {area_text}"));
    } else {
        sentences.push(format!("A {headline} is in effect for {area_text}"));
    }
    if !info.description.trim().is_empty() {
        sentences.push(info.description.trim().to_string());
    }
    if !info.instruction.trim().is_empty() {
        sentences.push(info.instruction.trim().to_string());
    }
    if info.description.trim().is_empty() && info.instruction.trim().is_empty() {
        sentences.push(format!("The alert was issued at {}.", alert.sent));
    }
    sentences.join(". ")
}

fn make_dispatch(
    delivery_id: &str,
    alert: &Alert,
    info: &AlertInfo,
    feed: &FeedConfig,
    info_group_id: String,
    parent_alert_id: String,
    kind: DispatchKind,
    locations: Vec<String>,
    newly_active_locations: Vec<String>,
    same_event: String,
    same_locations: Vec<String>,
    include_same: bool,
    title: String,
    text: String,
    received_at: &str,
) -> AlertDispatch {
    let key = format!("{delivery_id}|{}|{info_group_id}|{kind:?}", feed.id);
    AlertDispatch {
        decision_id: stable_id(&key),
        sequence: 0,
        ready_sent: false,
        request_sent: false,
        cancellation_applied: false,
        audio_path: String::new(),
        delivery_id: delivery_id.to_string(),
        alert_id: alert.identifier.clone(),
        parent_alert_id,
        info_group_id,
        feed_id: feed.id.clone(),
        kind,
        alert: alert.clone(),
        info: info.clone(),
        locations,
        newly_active_locations,
        cancelled_alert_ids: Vec::new(),
        same_event,
        same_locations,
        include_same,
        title,
        text,
        language: feed.language(),
        received_at: received_at.to_string(),
    }
}

pub(super) fn parse_cap_time(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn normalize_location(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|value| value.is_ascii_alphanumeric())
        .map(|value| value.to_ascii_uppercase())
        .collect()
}

fn is_same_code(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn xml_bool(raw: Option<&str>, fallback: bool) -> bool {
    raw.map(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        )
    })
    .unwrap_or(fallback)
}

pub(super) fn nonempty(primary: &str, secondary: &str, fallback: &str) -> String {
    [primary, secondary, fallback]
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or("Weather alert")
        .to_string()
}

fn stable_id(value: &str) -> String {
    format!("cap-{:x}", Sha256::digest(value.as_bytes()))
}

pub(super) fn first_text<'a>(message: &'a Value, data: &'a Value, keys: &[&str]) -> &'a str {
    keys.iter()
        .find_map(|key| {
            data.get(*key)
                .and_then(Value::as_str)
                .or_else(|| message.get(*key).and_then(Value::as_str))
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AlertFilterConfig, FeedAlertProviderConfig, FeedAlertsConfig, FeedCoverageConfig,
        FeedCoverageRegionConfig, FeedLocationsConfig,
    };

    fn feed() -> FeedConfig {
        FeedConfig {
            id: "sk-0001".to_string(),
            alerts: Some(FeedAlertsConfig {
                cap_cp: FeedAlertProviderConfig {
                    enabled: Some("true".to_string()),
                    filter: AlertFilterConfig::default(),
                },
                ..Default::default()
            }),
            locations: FeedLocationsConfig {
                coverage: FeedCoverageConfig {
                    regions: vec![FeedCoverageRegionConfig {
                        id: "065200".to_string(),
                    }],
                },
            },
            ..Default::default()
        }
    }

    fn alert(message_type: &str, urgency: &str, severity: &str, certainty: &str) -> Alert {
        let mut alert = Alert {
            identifier: "alert-1".to_string(),
            sender: "cap-pac@canada.ca".to_string(),
            sent: Utc::now().to_rfc3339(),
            status: "Actual".to_string(),
            message_type: message_type.to_string(),
            raw_xml: "<alert/>".to_string(),
            ..Default::default()
        };
        alert.infos.push(AlertInfo {
            language: "en-CA".to_string(),
            category: vec!["Met".to_string()],
            event: "Severe Thunderstorm Warning".to_string(),
            urgency: urgency.to_string(),
            severity: severity.to_string(),
            certainty: certainty.to_string(),
            headline: "Severe thunderstorm warning".to_string(),
            description: "A severe storm is moving through the area".to_string(),
            areas: vec![haze_cap::model::AlertArea {
                geocodes: vec![haze_cap::model::NameValue {
                    name: "layer:EC-MSC-SMC:1.0:CLC".to_string(),
                    value: "065200".to_string(),
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
        alert
    }

    #[test]
    fn only_new_qualified_locations_receive_priority_and_same() {
        let mut state = RouterState::default();
        let feeds = vec![feed()];
        let first = alert("Alert", "Expected", "Severe", "Likely");
        let decisions = route_document(&first, "delivery-1", "now", &feeds, &mut state);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].kind, DispatchKind::Priority);
        assert!(decisions[0].include_same);

        let update = alert("Update", "Expected", "Severe", "Likely");
        let decisions = route_document(&update, "delivery-2", "now", &feeds, &mut state);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].kind, DispatchKind::Routine);
        assert!(!decisions[0].include_same);
    }

    #[test]
    fn root_parent_survives_multiple_updates_and_cancellation() {
        let mut state = RouterState::default();
        let feeds = vec![feed()];
        let first = alert("Alert", "Expected", "Severe", "Likely");
        route_document(&first, "delivery-first", "now", &feeds, &mut state);

        let mut update_one = alert("Update", "Expected", "Severe", "Likely");
        update_one.identifier = "update-1".to_string();
        update_one.references = "sender,alert-1,2026-09-22T12:00:00Z".to_string();
        let first_update =
            route_document(&update_one, "delivery-update-1", "now", &feeds, &mut state);
        assert_eq!(first_update[0].parent_alert_id, "alert-1");

        let mut update_two = update_one.clone();
        update_two.identifier = "update-2".to_string();
        update_two.references = "sender,update-1,2026-09-22T12:05:00Z".to_string();
        let second_update =
            route_document(&update_two, "delivery-update-2", "now", &feeds, &mut state);
        assert_eq!(second_update[0].parent_alert_id, "alert-1");
        assert_eq!(second_update[0].kind, DispatchKind::Routine);

        let mut cancellation = alert("Cancel", "Immediate", "Extreme", "Observed");
        cancellation.identifier = "cancel-1".to_string();
        cancellation.references = "sender,update-1,2026-09-22T12:05:00Z".to_string();
        let cancelled = route_document(&cancellation, "delivery-cancel", "now", &feeds, &mut state);

        assert_eq!(cancelled.len(), 1);
        assert_eq!(cancelled[0].kind, DispatchKind::Cancellation);
        assert_eq!(cancelled[0].parent_alert_id, "alert-1");
        assert!(cancelled[0]
            .cancelled_alert_ids
            .contains(&"update-2".to_string()));
        assert!(state.active["sk-0001"].is_empty());
    }

    #[tokio::test]
    async fn urgent_lane_bypasses_routine_and_cancellation_retracts_lineage() {
        let temp = tempfile::tempdir().expect("temporary state directory");
        let pipeline = AlertPipeline::load(temp.path()).expect("load alert pipeline");
        let mut covered_feed = feed();
        covered_feed
            .locations
            .coverage
            .regions
            .push(FeedCoverageRegionConfig {
                id: "065201".to_string(),
            });
        let feeds = vec![covered_feed];

        let first = alert("Alert", "Future", "Minor", "Possible");
        pipeline
            .route(
                &json!({"delivery_id":"delivery-first", "data":{"delivery_id":"delivery-first", "alert":first}}),
                &feeds,
            )
            .await
            .expect("route routine alert");
        assert!(pipeline
            .next_dispatch_for_feed("sk-0001", true)
            .await
            .is_none());

        let mut update = alert("Update", "Immediate", "Extreme", "Observed");
        update.identifier = "update-1".to_string();
        update.references = "sender,alert-1,2026-09-22T12:00:00Z".to_string();
        update.infos[0].areas.push(haze_cap::model::AlertArea {
            threat_status: "issued".to_string(),
            geocodes: vec![haze_cap::model::NameValue {
                name: "layer:EC-MSC-SMC:1.0:CLC".to_string(),
                value: "065201".to_string(),
            }],
            ..Default::default()
        });
        pipeline
            .route(
                &json!({"delivery_id":"delivery-update", "data":{"delivery_id":"delivery-update", "alert":update}}),
                &feeds,
            )
            .await
            .expect("route urgent update");
        assert_eq!(
            pipeline
                .next_dispatch_for_feed("sk-0001", true)
                .await
                .expect("urgent update")
                .kind,
            DispatchKind::Priority
        );
        assert_eq!(
            pipeline
                .next_dispatch_for_feed("sk-0001", false)
                .await
                .expect("routine alert")
                .kind,
            DispatchKind::Routine
        );

        let mut cancellation = alert("Cancel", "Immediate", "Extreme", "Observed");
        cancellation.identifier = "cancel-1".to_string();
        cancellation.references = "sender,update-1,2026-09-22T12:05:00Z".to_string();
        let result = pipeline
            .route(
                &json!({"delivery_id":"delivery-cancel", "data":{"delivery_id":"delivery-cancel", "alert":cancellation}}),
                &feeds,
            )
            .await
            .expect("route cancellation");
        assert_eq!(result.dispatches.len(), 1);
        assert_eq!(
            super::super::dispatch_request_event(&result.dispatches[0])["data"]["alert_ids"],
            json!(["alert-1", "update-1"])
        );
        assert_eq!(
            pipeline.pending_dispatches().await.len(),
            1,
            "cancellation should retract pending routine and priority dispatches"
        );
        let cancellation_id = result.dispatches[0].decision_id.clone();
        assert_eq!(
            pipeline
                .next_dispatch_for_feed("sk-0001", true)
                .await
                .expect("urgent cancellation action")
                .kind,
            DispatchKind::Cancellation
        );
        assert!(pipeline
            .next_dispatch_for_feed("sk-0001", false)
            .await
            .is_none());
        pipeline
            .mark_cancellation_applied(&cancellation_id)
            .await
            .expect("persist cancellation retraction");
        assert_eq!(
            pipeline
                .next_dispatch_for_feed("sk-0001", false)
                .await
                .expect("cancellation speech")
                .kind,
            DispatchKind::Cancellation
        );
    }

    #[test]
    fn locationless_update_inherits_active_area_and_stays_routine() {
        let mut state = RouterState::default();
        let feeds = vec![feed()];
        let first = alert("Alert", "Expected", "Severe", "Likely");
        let initial = route_document(&first, "delivery-1", "now", &feeds, &mut state);
        assert_eq!(initial[0].kind, DispatchKind::Priority);

        let mut update = alert("Update", "Expected", "Severe", "Likely");
        update.references = "cap-pac@canada.ca,alert-1,2026-09-22T12:00:00Z".to_string();
        update.infos[0].areas.clear();
        let decisions = route_document(&update, "delivery-2", "now", &feeds, &mut state);

        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].kind, DispatchKind::Routine);
        assert!(!decisions[0].include_same);
        assert_eq!(decisions[0].locations, vec!["065200"]);
        assert!(decisions[0].newly_active_locations.is_empty());
    }

    #[test]
    fn update_with_only_out_of_coverage_locations_does_not_inherit_old_area() {
        let mut state = RouterState::default();
        let feeds = vec![feed()];
        let first = alert("Alert", "Expected", "Severe", "Likely");
        route_document(&first, "delivery-1", "now", &feeds, &mut state);

        let mut update = alert("Update", "Expected", "Severe", "Likely");
        update.references = "cap-pac@canada.ca,alert-1,2026-09-22T12:00:00Z".to_string();
        update.infos[0].areas[0].geocodes[0].value = "065201".to_string();
        let decisions = route_document(&update, "delivery-2", "now", &feeds, &mut state);

        assert!(decisions.is_empty());
        assert!(state.active["sk-0001"].is_empty());
    }

    #[test]
    fn expired_active_product_does_not_suppress_new_same() {
        let mut state = RouterState::default();
        let feeds = vec![feed()];
        let first = alert("Alert", "Expected", "Severe", "Likely");
        let initial = route_document(&first, "delivery-1", "now", &feeds, &mut state);
        assert_eq!(initial[0].kind, DispatchKind::Priority);
        state.active.get_mut("sk-0001").unwrap()[0].expires_at =
            (Utc::now() - Duration::minutes(1)).to_rfc3339();

        let mut update = alert("Update", "Expected", "Severe", "Likely");
        update.references = "cap-pac@canada.ca,alert-1,2026-09-22T12:00:00Z".to_string();
        let decisions = route_document(&update, "delivery-2", "now", &feeds, &mut state);

        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].kind, DispatchKind::Priority);
        assert_eq!(decisions[0].newly_active_locations, vec!["065200"]);
    }

    #[test]
    fn subthreshold_and_cancelled_messages_never_receive_same() {
        let mut state = RouterState::default();
        let feeds = vec![feed()];
        let low = alert("Alert", "Future", "Minor", "Possible");
        let decisions = route_document(&low, "delivery-low", "now", &feeds, &mut state);
        assert_eq!(decisions[0].kind, DispatchKind::Routine);
        assert!(!decisions[0].include_same);

        let mut cancelled = alert("Cancel", "Immediate", "Extreme", "Observed");
        cancelled.references = "sender,alert-1,2026-01-01T00:00:00Z".to_string();
        let decisions = route_document(&cancelled, "delivery-cancel", "now", &feeds, &mut state);
        assert!(decisions.iter().all(|decision| !decision.include_same));
        assert!(decisions
            .iter()
            .all(|decision| decision.kind == DispatchKind::Cancellation));
    }

    #[test]
    fn translations_with_same_hazard_and_scope_are_one_group() {
        let mut alert = alert("Alert", "Immediate", "Extreme", "Observed");
        alert.infos[0].event_codes.push(haze_cap::model::NameValue {
            name: "SAME".to_string(),
            value: "TOR".to_string(),
        });
        let mut french = alert.infos[0].clone();
        french.language = "fr-CA".to_string();
        french.event = "Avertissement de tornade".to_string();
        french.headline = "Alerte de tempete".to_string();
        alert.infos.push(french);
        assert_eq!(info_groups(&alert).len(), 1);
    }

    #[test]
    fn same_codes_include_only_new_codes_within_a_shared_area() {
        let mut alert = alert("Alert", "Immediate", "Extreme", "Observed");
        alert.infos[0].areas[0].geocodes = vec![
            haze_cap::model::NameValue {
                name: "layer:EC-MSC-SMC:1.0:CLC".to_string(),
                value: "065200".to_string(),
            },
            haze_cap::model::NameValue {
                name: "layer:EC-MSC-SMC:1.0:CLC".to_string(),
                value: "065201".to_string(),
            },
        ];

        let same = same_codes(&alert, &alert.infos[0], &feed(), &["065201".to_string()]);

        assert_eq!(same, vec!["065201"]);
    }

    #[test]
    fn distinct_hazards_with_the_same_scope_remain_separate_products() {
        let mut alert = alert("Alert", "Immediate", "Extreme", "Observed");
        let mut tornado = alert.infos[0].clone();
        tornado.event = "Tornado Warning".to_string();
        tornado.headline = "Tornado warning".to_string();
        alert.infos.push(tornado);

        let mut state = RouterState::default();
        let decisions = route_document(&alert, "delivery-hazards", "now", &[feed()], &mut state);
        assert_eq!(decisions.len(), 2);
        assert_ne!(decisions[0].info_group_id, decisions[1].info_group_id);
    }

    #[test]
    fn eccc_issued_area_tones_only_the_newly_active_location() {
        let mut feed = feed();
        feed.locations
            .coverage
            .regions
            .push(FeedCoverageRegionConfig {
                id: "065201".to_string(),
            });
        let mut state = RouterState::default();
        let first = alert("Alert", "Expected", "Severe", "Likely");
        route_document(
            &first,
            "delivery-eccc-first",
            "now",
            &[feed.clone()],
            &mut state,
        );

        let mut update = alert("Update", "Expected", "Severe", "Likely");
        update.references = "cap-pac@canada.ca,alert-1,2026-09-22T12:00:00Z".to_string();
        update.infos[0].areas = vec![
            haze_cap::model::AlertArea {
                geocodes: vec![haze_cap::model::NameValue {
                    name: "layer:EC-MSC-SMC:1.0:CLC".to_string(),
                    value: "065200".to_string(),
                }],
                ..Default::default()
            },
            haze_cap::model::AlertArea {
                threat_status: "issued".to_string(),
                geocodes: vec![haze_cap::model::NameValue {
                    name: "layer:EC-MSC-SMC:1.0:CLC".to_string(),
                    value: "065201".to_string(),
                }],
                ..Default::default()
            },
        ];
        let decisions = route_document(&update, "delivery-eccc-update", "now", &[feed], &mut state);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].newly_active_locations, vec!["065201"]);
        assert_eq!(decisions[0].same_locations, vec!["065201"]);
        assert_eq!(decisions[0].kind, DispatchKind::Priority);
    }

    #[test]
    fn nws_ugc_coverage_uses_exact_same_codes_for_priority() {
        let mut feed = feed();
        feed.alerts = Some(FeedAlertsConfig {
            cap_cp: FeedAlertProviderConfig {
                enabled: Some("false".to_string()),
                ..Default::default()
            },
            nws_cap: FeedAlertProviderConfig {
                enabled: Some("true".to_string()),
                filter: AlertFilterConfig::default(),
            },
        });
        feed.locations.coverage.regions = vec![FeedCoverageRegionConfig {
            id: "SKZ012".to_string(),
        }];
        let mut alert = alert("Alert", "Immediate", "Extreme", "Observed");
        alert.sender = "w-nws.webmaster@noaa.gov".to_string();
        alert.infos[0].event = "Tornado Warning".to_string();
        alert.infos[0].headline = "Tornado warning".to_string();
        alert.infos[0].event_codes = vec![haze_cap::model::NameValue {
            name: "SAME".to_string(),
            value: "TOR".to_string(),
        }];
        alert.infos[0].areas[0].geocodes = vec![
            haze_cap::model::NameValue {
                name: "UGC".to_string(),
                value: "SKZ012".to_string(),
            },
            haze_cap::model::NameValue {
                name: "SAME".to_string(),
                value: "065200".to_string(),
            },
        ];

        let mut state = RouterState::default();
        let decisions = route_document(&alert, "delivery-nws", "now", &[feed], &mut state);
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].kind, DispatchKind::Priority);
        assert_eq!(decisions[0].same_event, "TOR");
        assert_eq!(decisions[0].same_locations, vec!["065200"]);
    }

    #[tokio::test]
    async fn committed_router_outbox_replays_after_restart_and_dedupes_delivery() {
        let temp = tempfile::tempdir().expect("temporary state directory");
        let pipeline = AlertPipeline::load(temp.path()).expect("load new router state");
        let alert = alert("Alert", "Expected", "Severe", "Likely");
        let delivery_id = "stable-delivery";
        let event = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "delivery_id": delivery_id,
            "data": {
                "delivery_id": delivery_id,
                "alert": alert,
            }
        });
        let feeds = vec![feed()];

        let first = pipeline
            .route(&event, &feeds)
            .await
            .expect("commit delivery");
        assert!(!first.duplicate);
        assert_eq!(first.dispatches.len(), 1);
        pipeline
            .mark_ready_sent(&first.dispatches[0].decision_id)
            .await
            .expect("persist dispatched state");

        let restarted = AlertPipeline::load(temp.path()).expect("reload durable state");
        let replay = restarted.pending_dispatches().await;
        assert_eq!(replay.len(), 1);
        assert!(replay[0].ready_sent);
        let duplicate = restarted
            .route(&event, &feeds)
            .await
            .expect("dedupe redelivery");
        assert!(duplicate.duplicate);
        assert!(duplicate.dispatches.is_empty());
        assert_eq!(restarted.pending_dispatches().await.len(), 1);
    }

    #[tokio::test]
    async fn cancellation_retraction_ack_is_durable_and_unblocks_dispatch() {
        let temp = tempfile::tempdir().expect("temporary state directory");
        let pipeline =
            std::sync::Arc::new(AlertPipeline::load(temp.path()).expect("load alert pipeline"));
        let first_alert = alert("Alert", "Expected", "Severe", "Likely");
        let first_event = json!({
            "delivery_id": "delivery-first",
            "data": {
                "delivery_id": "delivery-first",
                "alert": first_alert,
            }
        });
        pipeline
            .route(&first_event, &[feed()])
            .await
            .expect("route active alert");

        let mut cancel = alert("Cancel", "Immediate", "Extreme", "Observed");
        cancel.references = "sender,alert-1,2026-09-22T12:00:00Z".to_string();
        let cancel_event = json!({
            "delivery_id": "delivery-cancel",
            "data": {
                "delivery_id": "delivery-cancel",
                "alert": cancel,
            }
        });
        let result = pipeline
            .route(&cancel_event, &[feed()])
            .await
            .expect("route cancellation");
        let decision_id = result.dispatches[0].decision_id.clone();
        assert_eq!(result.dispatches[0].kind, DispatchKind::Cancellation);
        assert!(!result.dispatches[0].cancellation_applied);

        let waiting_pipeline = std::sync::Arc::clone(&pipeline);
        let waiter = tokio::spawn(async move {
            waiting_pipeline
                .wait_cancellation_applied(&decision_id)
                .await
        });
        tokio::task::yield_now().await;
        pipeline
            .mark_cancellation_applied(&result.dispatches[0].decision_id)
            .await
            .expect("persist mixer acknowledgement");
        waiter
            .await
            .expect("cancellation waiter")
            .expect("unblocked");

        let restarted = AlertPipeline::load(temp.path()).expect("reload durable state");
        assert!(restarted.pending_dispatches().await[0].cancellation_applied);
    }
}
